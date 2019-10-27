package client

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Sherlock-Holo/tfs/api/rpc"
	"github.com/Sherlock-Holo/tfs/internal"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"
	errors "golang.org/x/xerrors"
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
	client *Client
	path   string
}

func NewRoot(client *Client) *Dir {
	return &Dir{
		path:   "/",
		client: client,
	}
}

func (d *Dir) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	req := &rpc.GetAttrRequest{
		Path: d.path,
	}

	var err error
	defer func() {
		if err != nil {
			err = errors.Errorf("get %s attr failed: %w", d.path, err)
			log.Errorf("%+v", err)
		}
	}()

	resp, err := d.client.GetAttr(ctx, req)
	if err != nil {
		return syscall.EIO
	}

	if respErr := resp.GetError(); respErr != nil {
		switch errResult := respErr.Err.(type) {
		case *rpc.Error_Errno:
			// err = syscall.Errno(errResult.Errno)
			return syscall.Errno(errResult.Errno)

		case *rpc.Error_Msg:
			err = errors.New(errResult.Msg)
			return syscall.EIO
		}
	}

	attr := resp.GetAttr()

	out.Mode = uint32(attr.Mode)
	out.Size = uint64(attr.Size)
	out.Owner = fuse.Owner{
		Uid: uint32(os.Getuid()),
		Gid: uint32(os.Getgid()),
	}
	out.Blksize = internal.BlockSize

	blocks := attr.Size / internal.BlockSize
	if attr.Size%internal.BlockSize != 0 {
		blocks++
	}
	out.Blocks = uint64(blocks)

	mtime, err := ptypes.Timestamp(attr.ModifyTime)
	if err != nil {
		err = errors.Errorf("get %s modify time failed: %w", d.path, err)
		log.Warnf("%v", err)
		mtime = time.Now() // don't return error
	}

	atime, err := ptypes.Timestamp(attr.AccessTime)
	if err != nil {
		err = errors.Errorf("get %s access time failed: %w", d.path, err)
		log.Warnf("%v", err)
		atime = time.Now() // don't return error
	}

	ctime, err := ptypes.Timestamp(attr.ChangeTime)
	if err != nil {
		err = errors.Errorf("get %s change time failed: %w", d.path, err)
		log.Warnf("%v", err)
		ctime = time.Now() // don't return error
	}

	internal.SetEntryOutTime(atime, mtime, ctime, &out.Attr)

	return fs.OK
}

func (d *Dir) Setattr(ctx context.Context, _ fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	var (
		mtime *timestamp.Timestamp
		atime *timestamp.Timestamp
		ctime *timestamp.Timestamp
		err   error
	)

	if modifyTime, ok := in.GetMTime(); ok {
		mtime, err = ptypes.TimestampProto(modifyTime)
		if err != nil {
			err = errors.Errorf("parse %s modify time failed: %w", d.path, err)
			log.Errorf("%+v", err)
			return syscall.EINVAL
		}
		out.Mtime = uint64(modifyTime.Unix())
		out.Mtimensec = uint32(modifyTime.UnixNano() - modifyTime.Unix()*1_000_000_000)
	}

	if accessTime, ok := in.GetATime(); ok {
		atime, err = ptypes.TimestampProto(accessTime)
		if err != nil {
			err = errors.Errorf("parse %s access time failed: %w", d.path, err)
			log.Errorf("%+v", err)
			return syscall.EINVAL
		}
		out.Atime = uint64(accessTime.Unix())
		out.Atimensec = uint32(accessTime.UnixNano() - accessTime.Unix()*1_000_000_000)
	}

	if changeTime, ok := in.GetCTime(); ok {
		ctime, err = ptypes.TimestampProto(changeTime)
		if err != nil {
			err = errors.Errorf("parse %s change time failed: %w", d.path, err)
			log.Errorf("%+v", err)
			return syscall.EINVAL
		}
		out.Ctime = uint64(changeTime.Unix())
		out.Ctimensec = uint32(changeTime.UnixNano() - changeTime.Unix()*1_000_000_000)
	}

	var (
		size int64 = -1
		mode int32 = -1
	)
	if newSize, ok := in.GetSize(); ok {
		size = int64(newSize)
		out.Size = newSize
	}
	if newMode, ok := in.GetMode(); ok {
		mode = int32(newMode)
		out.Mode = newMode
	}
	req := &rpc.SetAttrRequest{
		Path: d.path,
		Attr: &rpc.Attr{
			ModifyTime: mtime,
			AccessTime: atime,
			ChangeTime: ctime,
			Size:       size,
			Mode:       mode,
		},
	}

	defer func() {
		if err != nil {
			err = errors.Errorf("set %s attr failed: %w", d.path, err)
			log.Errorf("%+v", err)
		}
	}()

	resp, err := d.client.SetAttr(ctx, req)
	if err != nil {
		return syscall.EIO
	}

	if respErr := resp.GetError(); respErr != nil {
		switch errResult := respErr.Err.(type) {
		case *rpc.Error_Errno:
			// err = syscall.Errno(errResult.Errno)
			return syscall.Errno(errResult.Errno)

		case *rpc.Error_Msg:
			err = errors.New(errResult.Msg)
			return syscall.EIO
		}
	}

	return fs.OK
}

func (d *Dir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	req := &rpc.ReadDirRequest{
		DirPath: d.path,
	}

	var err error
	defer func() {
		if err != nil {
			err = errors.Errorf("read %s dir failed: %w", d.path, err)
			log.Errorf("%+v", err)
		}
	}()

	resp, err := d.client.ReadDir(ctx, req)
	if err != nil {
		return nil, syscall.EIO
	}

	if resp.Error != nil {
		switch errResult := resp.Error.Err.(type) {
		case *rpc.Error_Errno:
			// err = syscall.Errno(errResult.Errno)
			return nil, syscall.Errno(errResult.Errno)

		case *rpc.Error_Msg:
			err = errors.New(errResult.Msg)
			return nil, syscall.EIO
		}
	}

	dirEntries := make([]fuse.DirEntry, 0, len(resp.DirEntries))
	for _, respEntry := range resp.DirEntries {
		entry := fuse.DirEntry{
			Name: respEntry.Name,
		}

		switch respEntry.Type {
		case rpc.EntryType_dir:
			entry.Mode = syscall.S_IFDIR

		case rpc.EntryType_file:
			entry.Mode = syscall.S_IFREG
		}

		dirEntries = append(dirEntries, entry)
	}

	return fs.NewListDirStream(dirEntries), fs.OK
}

func (d *Dir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	req := &rpc.LookupRequest{
		DirPath:  d.path,
		Filename: name,
	}

	var err error
	defer func() {
		if err != nil {
			err = errors.Errorf("lookup %s in dir %s failed: %w", name, d.path, err)
			log.Errorf("%+v", err)
		}
	}()

	resp, err := d.client.Lookup(ctx, req)
	if err != nil {
		return nil, syscall.EIO
	}

	if respErr := resp.GetError(); respErr != nil {
		switch errResult := respErr.Err.(type) {
		case *rpc.Error_Errno:
			// err = syscall.Errno(errResult.Errno)
			return nil, syscall.Errno(errResult.Errno)

		case *rpc.Error_Msg:
			err = errors.New(errResult.Msg)
			return nil, syscall.EIO
		}
	}

	attr := resp.GetAttr()

	childNode := d.GetChild(name)
	if childNode == nil {
		var (
			child fs.InodeEmbedder
			sMode uint32
		)

		switch attr.Type {
		case rpc.EntryType_file:
			child = &File{
				client: d.client,
				path:   filepath.Join(d.path, name),
			}
			sMode = syscall.S_IFREG

		case rpc.EntryType_dir:
			child = &Dir{
				client: d.client,
				path:   filepath.Join(d.path, name),
			}
			sMode = syscall.S_IFDIR
		}

		childNode = d.NewInode(ctx, child, fs.StableAttr{Mode: sMode})
	}

	d.AddChild(name, childNode, true)

	out.Mode = uint32(attr.Mode)
	out.Size = uint64(attr.Size)
	out.Owner = fuse.Owner{
		Uid: uint32(os.Getuid()),
		Gid: uint32(os.Getgid()),
	}
	out.Blksize = internal.BlockSize

	blocks := attr.Size / internal.BlockSize
	if attr.Size%internal.BlockSize != 0 {
		blocks++
	}
	out.Blocks = uint64(blocks)

	mtime, err := ptypes.Timestamp(attr.ModifyTime)
	if err != nil {
		err = errors.Errorf("get %s modify time failed: %w", filepath.Join(d.path, name), err)
		log.Warnf("%v", err)
		mtime = time.Now() // don't return error
	}

	atime, err := ptypes.Timestamp(attr.AccessTime)
	if err != nil {
		err = errors.Errorf("get %s access time failed: %w", filepath.Join(d.path, name), err)
		log.Warnf("%v", err)
		atime = time.Now() // don't return error
	}

	ctime, err := ptypes.Timestamp(attr.ChangeTime)
	if err != nil {
		err = errors.Errorf("get %s change time failed: %w", filepath.Join(d.path, name), err)
		log.Warnf("%v", err)
		ctime = time.Now() // don't return error
	}

	internal.SetEntryOutTime(atime, mtime, ctime, &out.Attr)

	return childNode, fs.OK
}

func (d *Dir) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	req := &rpc.MkdirRequest{
		DirPath: d.path,
		NewName: name,
	}

	var err error
	defer func() {
		if err != nil {
			err = errors.Errorf("mkdir %s in dir %s failed: %w", name, d.path, err)
			log.Errorf("%+v", err)
		}
	}()

	resp, err := d.client.Mkdir(ctx, req)
	if err != nil {
		return nil, syscall.EIO
	}

	if respErr := resp.GetError(); respErr != nil {
		switch errResult := respErr.Err.(type) {
		case *rpc.Error_Errno:
			// err = syscall.Errno(errResult.Errno)
			return nil, syscall.Errno(errResult.Errno)

		case *rpc.Error_Msg:
			err = errors.New(errResult.Msg)
			return nil, syscall.EIO
		}
	}

	childNode := d.NewInode(ctx, &Dir{
		client: d.client,
		path:   filepath.Join(d.path, name),
	}, fs.StableAttr{Mode: syscall.S_IFDIR})

	d.AddChild(name, childNode, true)

	attr := resp.GetAttr()

	out.Mode = uint32(attr.Mode)
	out.Size = uint64(attr.Size)
	out.Owner = fuse.Owner{
		Uid: uint32(os.Getuid()),
		Gid: uint32(os.Getgid()),
	}
	out.Blksize = internal.BlockSize

	blocks := attr.Size / internal.BlockSize
	if attr.Size%internal.BlockSize != 0 {
		blocks++
	}
	out.Blocks = uint64(blocks)

	mtime, err := ptypes.Timestamp(attr.ModifyTime)
	if err != nil {
		log.Warnf("%+v", errors.Errorf("mkdir %s in dir %s has an error: %w", name, d.path, err))
		mtime = time.Now() // don't return error
	}

	atime, err := ptypes.Timestamp(attr.AccessTime)
	if err != nil {
		log.Warnf("%+v", errors.Errorf("mkdir %s in dir %s has an error: %w", name, d.path, err))
		atime = time.Now() // don't return error
	}

	ctime, err := ptypes.Timestamp(attr.ChangeTime)
	if err != nil {
		log.Warnf("%+v", errors.Errorf("mkdir %s in dir %s has an error: %w", name, d.path, err))
		ctime = time.Now() // don't return error
	}

	internal.SetEntryOutTime(atime, mtime, ctime, &out.Attr)

	return childNode, fs.OK
}

func (d *Dir) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	req := &rpc.CreateFileRequest{
		DirPath:  d.path,
		Filename: name,
		Mode:     mode,
	}

	var err error
	defer func() {
		if err != nil {
			err = errors.Errorf("create %s in dir %s failed: %w", name, d.path, err)
			log.Errorf("%+v", err)
		}
	}()

	resp, err := d.client.CreateFile(ctx, req)
	if err != nil {
		errno = syscall.EIO
		return
	}

	if respErr := resp.GetError(); respErr != nil {
		switch errResult := respErr.Err.(type) {
		case *rpc.Error_Errno:
			// err = syscall.Errno(errResult.Errno)
			return nil, nil, 0, syscall.Errno(errResult.Errno)

		case *rpc.Error_Msg:
			err = errors.New(errResult.Msg)
			return nil, nil, 0, syscall.EIO
		}
	}

	file := &File{
		path:   filepath.Join(d.path, name),
		client: d.client,
	}
	childNode := d.NewInode(ctx, file, fs.StableAttr{Mode: syscall.S_IFREG})

	d.AddChild(name, childNode, true)

	attr := resp.GetAttr()

	out.Mode = uint32(attr.Mode)
	out.Size = uint64(attr.Size)
	out.Owner = fuse.Owner{
		Uid: uint32(os.Getuid()),
		Gid: uint32(os.Getgid()),
	}
	out.Blksize = internal.BlockSize

	blocks := attr.Size / internal.BlockSize
	if attr.Size%internal.BlockSize != 0 {
		blocks++
	}
	out.Blocks = uint64(blocks)

	mtime, err := ptypes.Timestamp(attr.ModifyTime)
	if err != nil {
		log.Warnf("%+v", errors.Errorf("create file %s in dir %s has an error: %w", name, d.path, err))
		mtime = time.Now() // don't return error
	}

	atime, err := ptypes.Timestamp(attr.AccessTime)
	if err != nil {
		log.Warnf("%+v", errors.Errorf("create file %s in dir %s has an error: %w", name, d.path, err))
		atime = time.Now() // don't return error
	}

	ctime, err := ptypes.Timestamp(attr.ChangeTime)
	if err != nil {
		log.Warnf("%+v", errors.Errorf("create file %s in dir %s has an error: %w", name, d.path, err))
		ctime = time.Now() // don't return error
	}

	internal.SetEntryOutTime(atime, mtime, ctime, &out.Attr)

	handle := &fileHandle{
		file: file,
	}
	switch {
	case int(flags)&os.O_WRONLY > 0:
		handle.writable = true

	case int(flags)&os.O_RDWR > 0:
		handle.writable = true
		handle.readable = true

	default:
		// os.O_RDONLY
		handle.readable = true
	}

	return childNode, handle, 0, fs.OK
}

func (d *Dir) Unlink(ctx context.Context, name string) syscall.Errno {
	req := &rpc.UnlinkRequest{
		DirPath:  d.path,
		Filename: name,
	}

	var err error
	defer func() {
		if err != nil {
			err = errors.Errorf("unlink %s file in dir %s failed: %w", name, d.path, err)
			log.Errorf("%+v", err)
		}
	}()

	resp, err := d.client.Unlink(ctx, req)
	if err != nil {
		return syscall.EIO
	}

	if resp.Error != nil {
		switch errResult := resp.Error.Err.(type) {
		case *rpc.Error_Errno:
			// err = syscall.Errno(errResult.Errno)
			return syscall.Errno(errResult.Errno)

		case *rpc.Error_Msg:
			err = errors.New(errResult.Msg)
			return syscall.EIO
		}
	}

	return fs.OK
}

func (d *Dir) Rmdir(ctx context.Context, name string) syscall.Errno {
	req := &rpc.RmDirRequest{
		DirPath: d.path,
		RmName:  name,
	}

	var err error
	defer func() {
		if err != nil {
			err = errors.Errorf("rmdir %s in dir %s failed: %w", name, d.path, err)
			log.Errorf("%+v", err)
		}
	}()

	resp, err := d.client.RmDir(ctx, req)
	if err != nil {
		return syscall.EIO
	}

	if resp.Error != nil {
		switch errResult := resp.Error.Err.(type) {
		case *rpc.Error_Errno:
			// err = syscall.Errno(errResult.Errno)
			return syscall.Errno(errResult.Errno)

		case *rpc.Error_Msg:
			err = errors.New(errResult.Msg)
			return syscall.EIO
		}
	}

	return fs.OK
}

func (d *Dir) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	newDirPath := newParent.(*Dir).path

	req := &rpc.RenameRequest{
		DirPath:    d.path,
		OldName:    name,
		NewDirPath: newDirPath,
		NewName:    newName,
	}

	var err error
	defer func() {
		err = errors.Errorf("rename %s to %s failed: %w", filepath.Join(d.path, name), filepath.Join(newDirPath, newName))
		log.Errorf("%+v", err)
	}()

	resp, err := d.client.Rename(ctx, req)
	if err != nil {
		return syscall.EIO
	}

	if resp.Error != nil {
		switch errResult := resp.Error.Err.(type) {
		case *rpc.Error_Errno:
			// err = syscall.Errno(errResult.Errno)
			return syscall.Errno(errResult.Errno)

		case *rpc.Error_Msg:
			err = errors.New(errResult.Msg)
			return syscall.EIO
		}
	}

	return fs.OK
}
