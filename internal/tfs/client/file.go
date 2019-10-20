package client

import (
	"context"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/Sherlock-Holo/tfs/api/rpc"
	"github.com/Sherlock-Holo/tfs/internal/tfs"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"
	errors "golang.org/x/xerrors"
)

var (
	_ fs.NodeGetattrer = new(File)
	_ fs.NodeSetattrer = new(File)
	_ fs.NodeOpener    = new(File)
	_ fs.NodeAllocater = new(File)
	_ fs.NodeReader    = new(File)
	_ fs.NodeWriter    = new(File)
	_ fs.NodeReleaser  = new(File)
	_ fs.NodeFsyncer   = new(File)
)

type File struct {
	fs.Inode
	client *Client
	path   string
}

func (f *File) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	req := &rpc.GetAttrRequest{
		Path: f.path,
	}

	var err error
	defer func() {
		if err != nil {
			err = errors.Errorf("get %s attr failed: %w", f.path, err)
			log.Errorf("%+v", err)
		}
	}()

	resp, err := f.client.GetAttr(ctx, req)
	if err != nil {
		return syscall.EIO
	}

	if respErr := resp.GetError(); respErr != nil {
		switch errResult := respErr.Err.(type) {
		case *rpc.Error_Errno:
			err = syscall.Errno(errResult.Errno)
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
	out.Blksize = tfs.BlockSize

	blocks := attr.Size / tfs.BlockSize
	if attr.Size%tfs.BlockSize != 0 {
		blocks++
	}
	out.Blocks = uint64(blocks)

	mtime, err := ptypes.Timestamp(attr.ModifyTime)
	if err != nil {
		err = errors.Errorf("get %s modify time failed: %w", f.path, err)
		log.Warnf("%v", err)
		mtime = time.Now() // don't return error
	}

	atime, err := ptypes.Timestamp(attr.AccessTime)
	if err != nil {
		err = errors.Errorf("get %s access time failed: %w", f.path, err)
		log.Warnf("%v", err)
		atime = time.Now() // don't return error
	}

	ctime, err := ptypes.Timestamp(attr.ChangeTime)
	if err != nil {
		err = errors.Errorf("get %s change time failed: %w", f.path, err)
		log.Warnf("%v", err)
		ctime = time.Now() // don't return error
	}

	tfs.SetEntryOutTime(atime, mtime, ctime, &out.Attr)

	return fs.OK
}

func (f *File) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	var (
		mtime *timestamp.Timestamp
		atime *timestamp.Timestamp
		ctime *timestamp.Timestamp
		err   error
	)

	if modifyTime, ok := in.GetMTime(); ok {
		mtime, err = ptypes.TimestampProto(modifyTime)
		if err != nil {
			err = errors.Errorf("parse %s modify time failed: %w", f.path, err)
			log.Errorf("%+v", err)
			return syscall.EINVAL
		}
		out.Mtime = uint64(modifyTime.Unix())
		out.Mtimensec = uint32(modifyTime.UnixNano() - modifyTime.Unix()*1_000_000_000)
	}

	if accessTime, ok := in.GetATime(); ok {
		atime, err = ptypes.TimestampProto(accessTime)
		if err != nil {
			err = errors.Errorf("parse %s access time failed: %w", f.path, err)
			log.Errorf("%+v", err)
			return syscall.EINVAL
		}
		out.Atime = uint64(accessTime.Unix())
		out.Atimensec = uint32(accessTime.UnixNano() - accessTime.Unix()*1_000_000_000)
	}

	if changeTime, ok := in.GetCTime(); ok {
		ctime, err = ptypes.TimestampProto(changeTime)
		if err != nil {
			err = errors.Errorf("parse %s change time failed: %w", f.path, err)
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
	}
	if newMode, ok := in.GetMode(); ok {
		mode = int32(newMode)
	}
	req := &rpc.SetAttrRequest{
		Path: f.path,
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
			err = errors.Errorf("set %s attr failed: %w", f.path, err)
			log.Errorf("%+v", err)
		}
	}()

	resp, err := f.client.SetAttr(ctx, req)
	if err != nil {
		return syscall.EIO
	}

	if respErr := resp.GetError(); respErr != nil {
		switch errResult := respErr.Err.(type) {
		case *rpc.Error_Errno:
			err = syscall.Errno(errResult.Errno)
			return syscall.Errno(errResult.Errno)

		case *rpc.Error_Msg:
			err = errors.New(errResult.Msg)
			return syscall.EIO
		}
	}

	return fs.OK
}

func (f *File) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	req := &rpc.OpenFileRequest{
		Path: f.path,
	}

	var err error
	defer func() {
		if err != nil {
			err = errors.Errorf("open file %s failed: %w", f.path, err)
			log.Errorf("%+v", err)
		}
	}()

	resp, err := f.client.OpenFile(ctx, req)
	if err != nil {
		return nil, 0, syscall.EIO
	}

	if respErr := resp.GetError(); respErr != nil {
		switch errResult := respErr.Err.(type) {
		case *rpc.Error_Errno:
			err = syscall.Errno(errResult.Errno)
			return nil, 0, syscall.Errno(errResult.Errno)

		case *rpc.Error_Msg:
			err = errors.New(errResult.Msg)
			return nil, 0, syscall.EIO
		}
	}

	handle := &fileHandle{file: f}
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

	fh = handle
	errno = fs.OK

	return
}

func (f *File) Allocate(ctx context.Context, fh fs.FileHandle, offset uint64, size uint64, mode uint32) syscall.Errno {
	req := &rpc.AllocateRequest{
		Path:   f.path,
		Offset: offset,
		Size:   size,
	}

	var err error
	defer func() {
		if err != nil {
			err = errors.Errorf("allocate %s file offset %d size %d failed: %w", f.path, offset, size, err)
			log.Errorf("%+v", err)
		}
	}()

	resp, err := f.client.Allocate(ctx, req)
	if err != nil {
		return syscall.EIO
	}

	if resp.Error != nil {
		switch errResult := resp.Error.Err.(type) {
		case *rpc.Error_Errno:
			err = syscall.Errno(errResult.Errno)
			return syscall.Errno(errResult.Errno)

		case *rpc.Error_Msg:
			err = errors.New(errResult.Msg)
			return syscall.EIO
		}
	}

	return fs.OK
}

func (f *File) Read(ctx context.Context, fh fs.FileHandle, dest []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	if fh == nil {
		return nil, syscall.EBADFD
	}

	handle := fh.(*fileHandle)

	if !handle.readable {
		return nil, syscall.EBADFD
	}

	req := &rpc.ReadFileRequest{
		Path:   f.path,
		Offset: uint64(offset),
		Size:   uint64(len(dest)),
	}

	var err error
	defer func() {
		if err != nil {
			err = errors.Errorf("read file %s data failed: %w", f.path, err)
			log.Errorf("%+v", err)
		}
	}()

	respStream, err := f.client.ReadFile(ctx, req)
	if err != nil {
		return nil, syscall.EIO
	}

	var n uint64
	for {
		select {
		case <-respStream.Context().Done():
			err = respStream.Context().Err()
			switch {
			case errors.Is(err, context.Canceled):
				return nil, syscall.EINTR

			case errors.Is(err, context.DeadlineExceeded):
				return nil, syscall.ETIME

			default:
				return nil, syscall.EIO
			}

		default:
		}

		resp, err := respStream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, syscall.EIO
		}

		if respErr := resp.GetError(); respErr != nil {
			switch errResult := respErr.Err.(type) {
			case *rpc.Error_Errno:
				err = syscall.Errno(errResult.Errno)
				return nil, syscall.Errno(errResult.Errno)

			case *rpc.Error_Msg:
				err = errors.New(errResult.Msg)
				return nil, syscall.EIO
			}
		}

		n += uint64(copy(dest[n:], resp.GetData()))
	}

	return fuse.ReadResultData(dest[:n]), fs.OK
}

func (f *File) Write(ctx context.Context, fh fs.FileHandle, data []byte, offset int64) (written uint32, errno syscall.Errno) {
	if fh == nil {
		return 0, syscall.EBADFD
	}

	handle := fh.(*fileHandle)

	if !handle.writable {
		return 0, syscall.EBADFD
	}

	var err error
	defer func() {
		if err != nil {
			err = errors.Errorf("write %s file data failed: %w", f.path, err)
			log.Errorf("%+v", err)
		}
	}()

	writeStream, err := f.client.WriteFile(ctx)
	if err != nil {
		err = errors.Errorf("open write stream failed: %w", err)
		errno = syscall.EIO
		return
	}

	for _, d := range tfs.ChunkData(data) {
		select {
		case <-writeStream.Context().Done():
			err = writeStream.Context().Err()
			switch {
			case errors.Is(err, context.Canceled):
				errno = syscall.EINTR
				return

			case errors.Is(err, context.DeadlineExceeded):
				errno = syscall.ETIME
				return

			default:
				errno = syscall.EIO
				return
			}

		default:
		}

		req := &rpc.WriteFileRequest{
			Path:   f.path,
			Offset: uint64(written),
			Data:   d,
		}

		if err = writeStream.Send(req); err != nil {
			errno = syscall.EIO
			return
		}

		written += uint32(len(d))
	}

	resp, err := writeStream.CloseAndRecv()
	if err != nil {
		errno = syscall.EIO
		return
	}

	if respErr := resp.GetError(); respErr != nil {
		switch errResult := respErr.Err.(type) {
		case *rpc.Error_Errno:
			err = syscall.Errno(errResult.Errno)
			errno = syscall.Errno(errResult.Errno)
			return

		case *rpc.Error_Msg:
			err = errors.New(errResult.Msg)
			errno = syscall.EIO
			return
		}
	}

	respWritten := resp.GetWritten()
	if respWritten != uint64(written) {
		err = errors.New("written is not equal response written")
		errno = syscall.EIO
		return
	}

	errno = fs.OK

	return
}

func (f *File) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	if fh == nil {
		return syscall.EBADFD
	}

	return fs.OK
}

func (f *File) Fsync(ctx context.Context, fh fs.FileHandle, flags uint32) syscall.Errno {
	if fh == nil {
		return syscall.EBADFD
	}

	var err error
	defer func() {
		if err != nil {
			err = errors.Errorf("sync %s file failed: %w", f.path, err)
			log.Errorf("%+v", err)
		}
	}()

	respStream, err := f.client.SyncFile(ctx)
	if err != nil {
		return syscall.EIO
	}

	// ignore now
	_, err = respStream.CloseAndRecv()
	if err != nil {
		return syscall.EIO
	}

	return fs.OK
}
