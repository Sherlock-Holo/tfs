package client

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
			err = errors.WithMessage(err, fmt.Sprintf("get %s attr failed", d.path))
			log.Errorf("%+v", err)
		}
	}()

	resp, err := d.client.GetAttr(ctx, req)
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
		err = errors.Wrap(err, fmt.Sprintf("get %s modify time failed", d.path))
		log.Warnf("%v", err)
		mtime = time.Now() // don't return error
	}

	atime, err := ptypes.Timestamp(attr.AccessTime)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("get %s access time failed", d.path))
		log.Warnf("%v", err)
		atime = time.Now() // don't return error
	}

	ctime, err := ptypes.Timestamp(attr.ChangeTime)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("get %s change time failed", d.path))
		log.Warnf("%v", err)
		ctime = time.Now() // don't return error
	}

	tfs.SetEntryOutTime(atime, mtime, ctime, &out.Attr)

	return fs.OK
}

func (d *Dir) Setattr(ctx context.Context, _ fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	modifyTime := time.Unix(int64(in.Mtime), int64(in.Mtimensec))
	mtime, err := ptypes.TimestampProto(modifyTime)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("parse %s modify time failed", d.path))
		log.Errorf("%+v", err)
		return syscall.EINVAL
	}

	accessTime := time.Unix(int64(in.Atime), int64(in.Atimensec))
	atime, err := ptypes.TimestampProto(accessTime)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("parse %s access time failed", d.path))
		log.Errorf("%+v", err)
		return syscall.EINVAL
	}

	changeTime := time.Unix(int64(in.Ctime), int64(in.Ctimensec))
	ctime, err := ptypes.TimestampProto(changeTime)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("parse %s change time failed", d.path))
		log.Errorf("%+v", err)
		return syscall.EINVAL
	}

	req := &rpc.SetAttrRequest{
		Path: d.path,
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
			err = errors.WithMessage(err, fmt.Sprintf("set %s attr failed", d.path))
			log.Errorf("%+v", err)
		}
	}()

	resp, err := d.client.SetAttr(ctx, req)
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

func (d *Dir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	req := &rpc.ReadDirRequest{
		DirPath: d.path,
	}

	var err error
	defer func() {
		if err != nil {
			err = errors.WithMessage(err, fmt.Sprintf("read %s dir failed", d.path))
			log.Errorf("%+v", err)
		}
	}()

	resp, err := d.client.ReadDir(ctx, req)
	if err != nil {
		err = errors.WithStack(err)
		return nil, syscall.EIO
	}

	if resp.Error != nil {
		switch errResult := resp.Error.Err.(type) {
		case *rpc.Error_Errno:
			err = errors.WithStack(syscall.Errno(errResult.Errno))
			return nil, syscall.Errno(errResult.Errno)

		case *rpc.Error_Msg:
			err = errors.New(errResult.Msg)
			return nil, syscall.EIO
		}
	}

	dirEntries := make([]fuse.DirEntry, 0, len(resp.DirEntries))
	for _, entry := range resp.DirEntries {
		dirEntries = append(dirEntries, fuse.DirEntry{
			Name: entry.Name,
			Mode: entry.Mode,
		})
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
			err = errors.WithMessage(err, fmt.Sprintf("lookup %s in dir %s failed", name, d.path))
			log.Errorf("%+v", err)
		}
	}()

	resp, err := d.client.Lookup(ctx, req)
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
			sMode = syscall.S_IFDIR

		case rpc.EntryType_dir:
			child = &Dir{
				client: d.client,
				path:   filepath.Join(d.path, name),
			}
			sMode = syscall.S_IFREG
		}

		childNode = d.NewInode(ctx, child, fs.StableAttr{Mode: sMode})
	}

	d.AddChild(name, childNode, true)

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
			err = errors.WithMessage(err, fmt.Sprintf("mkdir %s in dir %s failed", name, d.path))
			log.Errorf("%+v", err)
		}
	}()

	resp, err := d.client.Mkdir(ctx, req)
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

	childNode := d.NewInode(ctx, &Dir{
		client: d.client,
		path:   filepath.Join(d.path, name),
	}, fs.StableAttr{Mode: syscall.S_IFDIR})

	d.AddChild(name, childNode, true)

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
		log.Warnf("%+v", errors.Wrap(err, fmt.Sprintf("mkdir %s in dir %s has an error", name, d.path)))
		mtime = time.Now() // don't return error
	}

	atime, err := ptypes.Timestamp(attr.AccessTime)
	if err != nil {
		log.Warnf("%+v", errors.Wrap(err, fmt.Sprintf("mkdir %s in dir %s has an error", name, d.path)))
		atime = time.Now() // don't return error
	}

	ctime, err := ptypes.Timestamp(attr.ChangeTime)
	if err != nil {
		log.Warnf("%+v", errors.Wrap(err, fmt.Sprintf("mkdir %s in dir %s has an error", name, d.path)))
		ctime = time.Now() // don't return error
	}

	tfs.SetEntryOutTime(atime, mtime, ctime, &out.Attr)

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
			err = errors.WithMessage(err, fmt.Sprintf("create %s in dir %s failed", name, d.path))
			log.Errorf("%+v", err)
		}
	}()

	resp, err := d.client.CreateFile(ctx, req)
	if err != nil {
		err = errors.WithStack(err)
		errno = syscall.EIO
		return
	}

	if respErr := resp.GetError(); respErr != nil {
		switch errResult := respErr.Err.(type) {
		case *rpc.Error_Errno:
			err = errors.WithStack(syscall.Errno(errResult.Errno))
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

	out.Mode = attr.Mode
	out.Size = attr.Size

	blocks := attr.Size / tfs.BlockSize
	if attr.Size%tfs.BlockSize != 0 {
		blocks++
	}
	out.Blocks = blocks

	mtime, err := ptypes.Timestamp(attr.ModifyTime)
	if err != nil {
		log.Warnf("%+v", errors.Wrap(err, fmt.Sprintf("create file %s in dir %s has an error", name, d.path)))
		mtime = time.Now() // don't return error
	}

	atime, err := ptypes.Timestamp(attr.AccessTime)
	if err != nil {
		log.Warnf("%+v", errors.Wrap(err, fmt.Sprintf("create file %s in dir %s has an error", name, d.path)))
		atime = time.Now() // don't return error
	}

	ctime, err := ptypes.Timestamp(attr.ChangeTime)
	if err != nil {
		log.Warnf("%+v", errors.Wrap(err, fmt.Sprintf("create file %s in dir %s has an error", name, d.path)))
		ctime = time.Now() // don't return error
	}

	tfs.SetEntryOutTime(atime, mtime, ctime, &out.Attr)

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
		if err == nil {
			err = errors.WithMessage(err, fmt.Sprintf("unlink %s file in dir %s failed", name, d.path))
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
			err = errors.WithStack(syscall.Errno(errResult.Errno))
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
			err = errors.WithMessage(err, fmt.Sprintf("rmdir %s in dir %s failed", name, d.path))
			log.Errorf("%+v", err)
		}
	}()

	resp, err := d.client.RmDir(ctx, req)
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
		err = errors.WithMessage(err, fmt.Sprintf("rename %s to %s failed", filepath.Join(d.path, name), filepath.Join(newDirPath, newName)))
		log.Errorf("%+v", err)
	}()

	resp, err := d.client.Rename(ctx, req)
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
