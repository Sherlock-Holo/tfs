package client

import (
	"context"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/Sherlock-Holo/tfs/api/rpc"
	"github.com/Sherlock-Holo/tfs/internal/tfs"
	"github.com/golang/protobuf/ptypes"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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
			err = errors.WithMessage(err, fmt.Sprintf("get %s attr failed", f.path))
			log.Errorf("%+v", err)
		}
	}()

	resp, err := f.client.GetAttr(ctx, req)
	if err != nil {
		err = errors.WithStack(err)
		return syscall.EIO
	}

	if respErr := resp.GetError(); respErr != nil {
		switch errResult := respErr.Err.(type) {
		case *rpc.Error_Errno:
			err = errors.WithStack(syscall.Errno(errResult.Errno))
			return syscall.Errno(errResult.Errno)

		case *rpc.Error_Msg:
			err = errors.New(errResult.Msg)
			return syscall.EIO
		}
	}

	attr := resp.GetAttr()

	out.Mode = attr.Mode
	out.Size = attr.Size

	blocks := attr.Size / tfs.BlockSize
	if attr.Size%tfs.BlockSize != 0 {
		blocks++
	}
	out.Blocks = blocks

	mtime, err := ptypes.Timestamp(attr.ModifyTime)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("get %s modify time failed", f.path))
		log.Warnf("%v", err)
		mtime = time.Now() // don't return error
	}

	atime, err := ptypes.Timestamp(attr.AccessTime)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("get %s access time failed", f.path))
		log.Warnf("%v", err)
		atime = time.Now() // don't return error
	}

	ctime, err := ptypes.Timestamp(attr.ChangeTime)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("get %s change time failed", f.path))
		log.Warnf("%v", err)
		ctime = time.Now() // don't return error
	}

	tfs.SetEntryOutTime(atime, mtime, ctime, &out.Attr)

	return fs.OK
}

func (f *File) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	modifyTime := time.Unix(int64(in.Mtime), int64(in.Mtimensec))
	mtime, err := ptypes.TimestampProto(modifyTime)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("parse %s modify time failed", f.path))
		log.Errorf("%+v", err)
		return syscall.EINVAL
	}

	accessTime := time.Unix(int64(in.Atime), int64(in.Atimensec))
	atime, err := ptypes.TimestampProto(accessTime)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("parse %s access time failed", f.path))
		log.Errorf("%+v", err)
		return syscall.EINVAL
	}

	changeTime := time.Unix(int64(in.Ctime), int64(in.Ctimensec))
	ctime, err := ptypes.TimestampProto(changeTime)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("parse %s change time failed", f.path))
		log.Errorf("%+v", err)
		return syscall.EINVAL
	}

	req := &rpc.SetAttrRequest{
		Path: f.path,
		Attr: &rpc.Attr{
			ModifyTime: mtime,
			AccessTime: atime,
			ChangeTime: ctime,
			Size:       in.Size,
			Mode:       in.Mode,
		},
	}

	defer func() {
		if err != nil {
			err = errors.WithMessage(err, fmt.Sprintf("set %s attr failed", f.path))
			log.Errorf("%+v", err)
		}
	}()

	resp, err := f.client.SetAttr(ctx, req)
	if err != nil {
		err = errors.WithStack(err)
		return syscall.EIO
	}

	if respErr := resp.GetError(); respErr != nil {
		switch errResult := respErr.Err.(type) {
		case *rpc.Error_Errno:
			err = errors.WithStack(syscall.Errno(errResult.Errno))
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
			err = errors.WithMessage(err, fmt.Sprintf("open file %s failed", f.path))
			log.Errorf("%+v", err)
		}
	}()

	resp, err := f.client.OpenFile(ctx, req)
	if err != nil {
		err = errors.WithStack(err)
		return nil, 0, syscall.EIO
	}

	if respErr := resp.GetError(); respErr != nil {
		switch errResult := respErr.Err.(type) {
		case *rpc.Error_Errno:
			err = errors.WithStack(syscall.Errno(errResult.Errno))
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
			err = errors.WithMessage(err, fmt.Sprintf("allocate %s file offset %d size %d failed", f.path, offset, size))
			log.Errorf("%+v", err)
		}
	}()

	resp, err := f.client.Allocate(ctx, req)
	if err != nil {
		err = errors.WithStack(err)
		return syscall.EIO
	}

	if resp.Error != nil {
		switch errResult := resp.Error.Err.(type) {
		case *rpc.Error_Errno:
			err = errors.WithStack(syscall.Errno(errResult.Errno))
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
			err = errors.WithMessage(err, fmt.Sprintf("read file %s data failed", f.path))
			log.Errorf("%+v", err)
		}
	}()

	respStream, err := f.client.ReadFile(ctx, req)
	if err != nil {
		err = errors.WithStack(err)
		return nil, syscall.EIO
	}

	var n uint64
	for {
		select {
		case <-respStream.Context().Done():
			err = errors.WithStack(respStream.Context().Err())
			switch respStream.Context().Err() {
			case context.Canceled:
				return nil, syscall.EINTR

			case context.DeadlineExceeded:
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
			err = errors.WithStack(err)
			return nil, syscall.EIO
		}

		if respErr := resp.GetError(); respErr != nil {
			switch errResult := respErr.Err.(type) {
			case *rpc.Error_Errno:
				err = errors.WithStack(syscall.Errno(errResult.Errno))
				return nil, syscall.Errno(errResult.Errno)

			case *rpc.Error_Msg:
				err = errors.New(errResult.Msg)
				return nil, syscall.EIO
			}
		}

		data := resp.GetData()
		n += uint64(copy(dest[n:], data))
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
			err = errors.WithMessage(err, fmt.Sprintf("write %s file data failed", f.path))
			log.Errorf("%+v", err)
		}
	}()

	writeStream, err := f.client.WriteFile(ctx)
	if err != nil {
		err = errors.WithStack(err)
		errno = syscall.EIO
		return
	}

	for _, d := range tfs.ChunkData(data) {
		select {
		case <-writeStream.Context().Done():
			err = errors.WithStack(writeStream.Context().Err())
			switch writeStream.Context().Err() {
			case context.Canceled:
				errno = syscall.EINTR
				return

			case context.DeadlineExceeded:
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
			err = errors.WithStack(err)
			errno = syscall.EIO
			return
		}

		written += uint32(len(d))
	}

	resp, err := writeStream.CloseAndRecv()
	if err != nil {
		err = errors.WithStack(err)
		errno = syscall.EIO
		return
	}

	if respErr := resp.GetError(); respErr != nil {
		switch errResult := respErr.Err.(type) {
		case *rpc.Error_Errno:
			err = errors.WithStack(syscall.Errno(errResult.Errno))
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

	req := &rpc.CloseFileRequest{
		Path: f.path,
	}

	var err error
	defer func() {
		if err != nil {
			err = errors.WithMessage(err, fmt.Sprintf("close %s file failed", f.path))
			log.Errorf("%+v", err)
		}
	}()

	resp, err := f.client.CloseFile(ctx, req)
	if err != nil {
		err = errors.WithStack(err)
		return syscall.EIO
	}

	if resp.Error != nil {
		switch errResult := resp.Error.Err.(type) {
		case *rpc.Error_Errno:
			err = errors.WithStack(syscall.Errno(errResult.Errno))
			return syscall.Errno(errResult.Errno)

		case *rpc.Error_Msg:
			err = errors.New(errResult.Msg)
			return syscall.EIO
		}
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
			err = errors.WithMessage(err, fmt.Sprintf("sync %s file failed", f.path))
			log.Errorf("%+v", err)
		}
	}()

	respStream, err := f.client.SyncFile(ctx)
	if err != nil {
		err = errors.WithStack(err)
		return syscall.EIO
	}

	// ignore now
	_, err = respStream.CloseAndRecv()
	if err != nil {
		err = errors.WithStack(err)
		return syscall.EIO
	}

	return fs.OK
}
