package memfs

import (
	"context"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

var (
	_ fs.NodeGetattrer = new(Dir)
	_ fs.NodeSetattrer = new(Dir)
	_ fs.NodeReaddirer = new(Dir)
	_ fs.NodeLookuper  = new(Dir)
	_ fs.NodeMkdirer   = new(Dir)
	_ fs.NodeCreater   = new(Dir)
	_ fs.NodeUnlinker  = new(Dir)
	_ fs.NodeRmdirer   = new(Dir)
	_ fs.NodeRenamer   = new(Dir)
)

type Dir struct {
	fs.Inode
	mutex      *sync.RWMutex
	name       string
	accessTime time.Time
	modifyTime time.Time
	changeTime time.Time
	mode       os.FileMode
	owner      fuse.Owner
}

func NewRoot(owner *fuse.Owner) *Dir {
	now := time.Now()

	root := &Dir{
		name:       "/",
		mutex:      new(sync.RWMutex),
		accessTime: now,
		modifyTime: now,
		changeTime: now,
		mode:       os.ModeDir | 0755,
	}

	if owner != nil {
		root.owner = *owner
	}

	root.EmbeddedInode()

	return root
}

func (d *Dir) Rmdir(ctx context.Context, name string) syscall.Errno {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	childNode := d.GetChild(name)
	if childNode == nil {
		return syscall.ENOENT
	}

	if !childNode.IsDir() {
		return syscall.ENOTEMPTY
	}

	return fs.OK
}

func (d *Dir) Unlink(ctx context.Context, name string) syscall.Errno {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	childNode := d.GetChild(name)
	if childNode == nil {
		return syscall.ENOENT
	}

	return fs.OK
}

func (d *Dir) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	out.Mode = uint32(d.mode)

	setEntryOutTime(d.accessTime, d.modifyTime, d.changeTime, &out.Attr)

	return fs.OK
}

func (d *Dir) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if mode, ok := in.GetMode(); ok {
		d.mode = os.FileMode(mode)
		out.Mode = uint32(d.mode)
	}

	if atime, ok := in.GetATime(); ok {
		d.accessTime = atime
		out.Atime = uint64(d.accessTime.Unix())
		out.Atimensec = uint32(d.accessTime.UnixNano() - d.accessTime.Unix()*1_000_000_000)
	}

	if mtime, ok := in.GetMTime(); ok {
		d.modifyTime = mtime
		out.Mtime = uint64(d.modifyTime.Unix())
		out.Mtimensec = uint32(d.modifyTime.UnixNano() - d.modifyTime.Unix()*1_000_000_000)
	}

	if ctime, ok := in.GetCTime(); ok {
		d.changeTime = ctime
		out.Ctime = uint64(d.changeTime.Unix())
		out.Ctimensec = uint32(d.changeTime.UnixNano() - d.changeTime.Unix()*1_000_000_000)
	}

	owner := d.owner
	if uid, ok := in.GetUID(); ok {
		owner.Uid = uid
	}
	if gid, ok := in.GetGID(); ok {
		owner.Gid = gid
	}
	d.owner = owner
	out.Owner = d.owner

	return fs.OK
}

func (d *Dir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	entries := make([]fuse.DirEntry, 0, 2)

	_, parent := d.Parent()
	if d.IsRoot() {
		parent = &d.Inode
	}
	entries = append(entries,
		fuse.DirEntry{
			Name: ".",
			Ino:  d.StableAttr().Ino,
			Mode: d.Inode.Mode(),
		},
		fuse.DirEntry{
			Name: "..",
			Ino:  parent.StableAttr().Ino,
			Mode: parent.Mode(),
		},
	)

	for name, inode := range d.Children() {
		switch name {
		case ".", "..":
			continue

		default:
			entries = append(entries, fuse.DirEntry{
				Ino:  inode.StableAttr().Ino,
				Mode: inode.Mode(),
				Name: name,
			})
		}
	}

	return fs.NewListDirStream(entries), fs.OK
}

func (d *Dir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if name == "." {
		return &d.Inode, fs.OK
	}

	if name == ".." {
		if d.IsRoot() {
			return &d.Inode, fs.OK
		}
		_, parent := d.Parent()
		if parent == nil {
			return nil, syscall.ENOENT
		}
		return parent, fs.OK
	}

	childInode := d.GetChild(name)
	if childInode == nil {
		return nil, syscall.ENOENT
	}

	out.Ino = childInode.StableAttr().Ino

	child := childInode.Operations()
	if childInode.IsDir() {
		dir := child.(*Dir)

		out.Mode = uint32(dir.mode)
		out.Owner = dir.owner

		setEntryOutTime(dir.accessTime, dir.modifyTime, dir.changeTime, &out.Attr)
	} else {
		file := child.(*File)

		out.Mode = uint32(file.mode)
		out.Size = uint64(len(file.content))
		out.Owner = file.owner
		out.Blksize = 512

		blocks := out.Size / 512
		if out.Size%512 != 0 {
			blocks++
		}
		out.Blocks = blocks

		setEntryOutTime(file.accessTime, file.modifyTime, file.changeTime, &out.Attr)
	}

	return childInode, fs.OK
}

func (d *Dir) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.GetChild(name) != nil {
		return nil, syscall.EEXIST
	}

	now := time.Now()

	d.accessTime = now
	d.modifyTime = now

	dir := &Dir{
		name:       name,
		mutex:      new(sync.RWMutex),
		accessTime: now,
		modifyTime: now,
		changeTime: now,
		owner:      d.owner,
		mode:       os.FileMode(mode),
	}

	out.Mode = uint32(dir.mode)
	out.Owner = dir.owner
	setEntryOutTime(dir.accessTime, dir.modifyTime, dir.changeTime, &out.Attr)

	dirNode := d.NewInode(ctx, dir, fs.StableAttr{Mode: syscall.S_IFDIR})
	if !d.AddChild(name, dirNode, false) {
		panic("child node exists, it should not happened")
	}

	out.Ino = dirNode.StableAttr().Ino

	return dirNode, fs.OK
}

func (d *Dir) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.GetChild(name) != nil {
		return nil, nil, 0, syscall.EEXIST
	}

	now := time.Now()

	d.accessTime = now
	d.modifyTime = now

	file := &File{
		name:       name,
		mutex:      new(sync.RWMutex),
		mode:       os.FileMode(mode),
		owner:      d.owner,
		accessTime: now,
		modifyTime: now,
		changeTime: now,
	}

	out.Mode = uint32(file.mode)
	out.Owner = file.owner
	setEntryOutTime(file.accessTime, file.modifyTime, file.changeTime, &out.Attr)

	fileNode := d.NewInode(ctx, file, fs.StableAttr{Mode: syscall.S_IFREG})
	if !d.AddChild(name, fileNode, false) {
		panic("child node exists, it should not happened")
	}

	out.Ino = fileNode.StableAttr().Ino

	return fileNode, &fileHandle{
		file:     file,
		writable: flags&uint32(os.O_RDWR) > 0 || flags&uint32(os.O_WRONLY) > 0,
		readable: flags&uint32(os.O_RDWR) > 0 || flags&uint32(os.O_RDONLY) > 0,
	}, 0, fs.OK
}

func (d *Dir) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	return fs.OK
}
