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
	_ fs.NodeGetattrer = new(File)
	_ fs.NodeSetattrer = new(File)
	_ fs.NodeOpener    = new(File)
	_ fs.NodeReader    = new(File)
	_ fs.NodeWriter    = new(File)
	_ fs.NodeReleaser  = new(File)
	_ fs.NodeFsyncer   = new(File)
)

type File struct {
	fs.Inode
	mutex      *sync.RWMutex
	name       string
	accessTime time.Time
	modifyTime time.Time
	changeTime time.Time
	mode       os.FileMode
	owner      fuse.Owner
	content    []byte
}

func (f *File) Getattr(ctx context.Context, handle fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	defer func() {
		f.accessTime = time.Now()
	}()

	out.Mode = uint32(f.mode)
	out.Owner = f.owner
	out.Size = uint64(len(f.content))
	out.Blksize = 512

	blocks := out.Size / 512
	if out.Size%512 != 0 {
		blocks++
	}
	out.Blocks = blocks

	setEntryOutTime(f.accessTime, f.modifyTime, f.changeTime, &out.Attr)

	return fs.OK
}

func (f *File) Setattr(ctx context.Context, handle fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	defer func() {
		now := time.Now()
		f.accessTime = now
		f.changeTime = now
	}()

	if mode, ok := in.GetMode(); ok {
		f.mode = os.FileMode(mode)
		out.Mode = uint32(f.mode)
	}

	if atime, ok := in.GetATime(); ok {
		f.accessTime = atime
		out.Atime = uint64(f.accessTime.Unix())
		out.Atimensec = uint32(f.accessTime.UnixNano() - f.accessTime.Unix()*1_000_000_000)
	}

	if mtime, ok := in.GetMTime(); ok {
		f.modifyTime = mtime
		out.Mtime = uint64(f.modifyTime.Unix())
		out.Mtimensec = uint32(f.modifyTime.UnixNano() - f.modifyTime.Unix()*1_000_000_000)
	}

	if ctime, ok := in.GetCTime(); ok {
		f.changeTime = ctime
		out.Ctime = uint64(f.changeTime.Unix())
		out.Ctimensec = uint32(f.changeTime.UnixNano() - f.changeTime.Unix()*1_000_000_000)
	}

	owner := f.owner
	if uid, ok := in.GetUID(); ok {
		owner.Uid = uid
	}
	if gid, ok := in.GetGID(); ok {
		owner.Gid = gid
	}
	f.owner = owner
	out.Owner = f.owner

	if size, ok := in.GetSize(); ok {
		f.content = f.content[:size]
		out.Size = uint64(len(f.content))
	}

	return fs.OK
}

type fileHandle struct {
	file     *File
	writable bool
}

func (f *File) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	handle := &fileHandle{
		file:     f,
		writable: flags&uint32(os.O_RDWR) > 0 || flags&uint32(os.O_WRONLY) > 0,
	}

	return handle, 0, fs.OK
}

func (f *File) Write(ctx context.Context, handle fs.FileHandle, data []byte, offset int64) (written uint32, errno syscall.Errno) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	defer func() {
		now := time.Now()
		f.accessTime = now
		f.modifyTime = now
		f.changeTime = now
	}()

	fileHandle, ok := handle.(*fileHandle)
	if !ok {
		return 0, syscall.EBADF
	}

	if !fileHandle.writable {
		return 0, syscall.EBADF
	}

	if offset > int64(len(f.content)) {
		f.content = append(f.content, data...)
		return uint32(len(data)), fs.OK
	}

	n := copy(f.content[offset:], data)
	if n < len(data) {
		f.content = append(f.content, data[n:]...)
	}

	return uint32(len(data)), fs.OK
}

func (f *File) Read(ctx context.Context, handle fs.FileHandle, dest []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	defer func() {
		f.accessTime = time.Now()
	}()

	if offset >= int64(len(f.content)) {
		return fuse.ReadResultData(make([]byte, 0)), fs.OK
	}

	end := int64(len(dest)) + offset
	if end > int64(len(f.content)) {
		end = int64(len(f.content))
	}

	buffer := make([]byte, end-offset)
	copy(buffer, f.content[offset:end])

	return fuse.ReadResultData(buffer), fs.OK
}

func (f *File) Release(ctx context.Context, handle fs.FileHandle) syscall.Errno {
	return fs.OK
}

func (f *File) Fsync(ctx context.Context, _ fs.FileHandle, flags uint32) syscall.Errno {
	return fs.OK
}
