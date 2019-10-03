package server

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"

	"github.com/Sherlock-Holo/tfs/api/rpc"
	"github.com/Sherlock-Holo/tfs/internal/tfs"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
)

type Server struct {
	Root  string
	files map[string]*file
}

func (s *Server) Nothing(ctx context.Context, _ *empty.Empty) (*empty.Empty, error) {
	return new(empty.Empty), nil
}

func (s *Server) ReadDir(ctx context.Context, req *rpc.ReadDirRequest) (resp *rpc.ReadDirResponse, errRet error) {
	resp = new(rpc.ReadDirResponse)

	path := filepath.Join(s.Root, req.DirPath)
	infos, err := ioutil.ReadDir(path)
	switch err {
	case os.ErrNotExist:
		resp.Error = &rpc.Error{
			Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)},
		}
		return

	case os.ErrPermission:
		resp.Error = &rpc.Error{
			Err: &rpc.Error_Errno{Errno: uint32(syscall.EPERM)},
		}
		return

	default:
		resp.Error = &rpc.Error{
			Err: &rpc.Error_Msg{Msg: err.Error()},
		}
		return

	case nil:
	}

	for _, info := range infos {
		dirEntry := &rpc.ReadDirResponse_DirEntry{
			Name: info.Name(),
			Mode: uint32(info.Mode()),
		}

		if info.IsDir() {
			dirEntry.Type = rpc.EntryType_dir
		} else {
			dirEntry.Type = rpc.EntryType_file
		}

		resp.DirEntries = append(resp.DirEntries, dirEntry)
	}

	return
}

func (s *Server) Lookup(ctx context.Context, req *rpc.LookupRequest) (resp *rpc.LookupResponse, errRet error) {
	resp = new(rpc.LookupResponse)

	path := filepath.Join(s.Root, req.DirPath, req.Filename)
	info, err := os.Stat(path)
	switch err {
	case os.ErrNotExist:
		resp.Result = &rpc.LookupResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)},
			},
		}
		return

	case os.ErrPermission:
		resp.Result = &rpc.LookupResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Errno{Errno: uint32(syscall.EPERM)},
			},
		}
		return

	default:
		resp.Result = &rpc.LookupResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Msg{Msg: err.Error()},
			},
		}
		return

	case nil:
	}

	resp.Result = &rpc.LookupResponse_Attr{
		Attr: tfs.CreateAttr(info),
	}

	return
}

func (s *Server) Mkdir(ctx context.Context, req *rpc.MkdirRequest) (resp *rpc.MkdirResponse, errRet error) {
	resp = new(rpc.MkdirResponse)

	path := filepath.Join(s.Root, req.DirPath, req.NewName)
	_, err := os.Stat(path)
	switch err {
	case nil:
		resp.Result = &rpc.MkdirResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Errno{Errno: uint32(syscall.EEXIST)},
			},
		}
		return

	default:
		resp.Result = &rpc.MkdirResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Msg{Msg: err.Error()},
			},
		}
		return

	case os.ErrNotExist:
	}

	// TODO: handle concurrent
	if err := os.Mkdir(path, 0644|os.ModeDir); err != nil {
		resp.Result = &rpc.MkdirResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Errno{Errno: uint32(syscall.EIO)},
			},
		}
		return
	}

	// TODO: handle concurrent
	info, err := os.Stat(path)
	if err != nil {
		resp.Result = &rpc.MkdirResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Errno{Errno: uint32(syscall.EIO)},
			},
		}
		return
	}

	resp.Result = &rpc.MkdirResponse_Attr{
		Attr: tfs.CreateAttr(info),
	}

	return
}

func (s *Server) CreateFile(ctx context.Context, req *rpc.CreateFileRequest) (resp *rpc.CreateFileResponse, errRet error) {
	resp = new(rpc.CreateFileResponse)

	path := filepath.Join(s.Root, req.DirPath, req.Filename)
	_, err := os.Stat(path)
	switch err {
	case nil:
		resp.Result = &rpc.CreateFileResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Errno{Errno: uint32(syscall.EEXIST)},
			},
		}
		return

	case os.ErrPermission:
		resp.Result = &rpc.CreateFileResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Errno{Errno: uint32(syscall.EPERM)},
			},
		}
		return

	default:
		resp.Result = &rpc.CreateFileResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Msg{Msg: err.Error()},
			},
		}
		return

	case os.ErrNotExist:
	}

	// TODO: handle concurrent
	file, err := os.OpenFile(path, os.O_CREATE, os.FileMode(req.Mode))
	if err != nil {
		resp.Result = &rpc.CreateFileResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Errno{Errno: uint32(syscall.EIO)},
			},
		}
		return
	}
	defer func() {
		_ = file.Close()
	}()

	// TODO: handle concurrent
	info, err := file.Stat()
	if err != nil {
		resp.Result = &rpc.CreateFileResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Errno{Errno: uint32(syscall.EIO)},
			},
		}
		return
	}

	resp.Result = &rpc.CreateFileResponse_Attr{
		Attr: tfs.CreateAttr(info),
	}

	return
}

func (s *Server) Unlink(ctx context.Context, req *rpc.UnlinkRequest) (resp *rpc.UnlinkResponse, errRet error) {
	resp = new(rpc.UnlinkResponse)

	path := filepath.Join(s.Root, req.DirPath, req.Filename)
	resp.Error = rmFileOrDir(path)

	return
}

func (s *Server) RmDir(ctx context.Context, req *rpc.RmDirRequest) (resp *rpc.RmDirResponse, errRet error) {
	resp = new(rpc.RmDirResponse)

	path := filepath.Join(s.Root, req.DirPath, req.RmName)
	if err := rmFileOrDir(path); err != nil {
		resp.Error = err
	}

	return
}

func (s *Server) Rename(ctx context.Context, req *rpc.RenameRequest) (resp *rpc.RenameResponse, errRet error) {
	resp = new(rpc.RenameResponse)

	oldPath := filepath.Join(s.Root, req.DirPath, req.OldName)
	newPath := filepath.Join(s.Root, req.NewDirPath, req.NewName)

	_, err := os.Stat(newPath)
	switch err {
	case nil:
		resp.Error = &rpc.Error{
			Err: &rpc.Error_Errno{Errno: uint32(syscall.EEXIST)},
		}
		return

	case os.ErrPermission:
		resp.Error = &rpc.Error{
			Err: &rpc.Error_Errno{Errno: uint32(syscall.EPERM)},
		}
		return

	default:
		resp.Error = &rpc.Error{
			Err: &rpc.Error_Msg{Msg: err.Error()},
		}
		return

	case os.ErrNotExist:
	}

	err = os.Rename(oldPath, newPath)
	switch err {
	case os.ErrNotExist:
		resp.Error = &rpc.Error{
			Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)},
		}
		return

	case os.ErrPermission:
		resp.Error = &rpc.Error{
			Err: &rpc.Error_Errno{Errno: uint32(syscall.EPERM)},
		}
		return

	default:
		resp.Error = &rpc.Error{
			Err: &rpc.Error_Msg{Msg: err.Error()},
		}
		return

	case nil:
		return
	}
}

func (s *Server) OpenFile(ctx context.Context, req *rpc.OpenFileRequest) (resp *rpc.OpenFileResponse, errRet error) {
	resp = new(rpc.OpenFileResponse)

	path := filepath.Join(s.Root, req.Path)
	if file, ok := s.files[path]; ok {
		info, err := file.f.Stat()
		switch err {
		case os.ErrNotExist:
			resp.Result = &rpc.OpenFileResponse_Error{
				Error: &rpc.Error{
					Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)},
				},
			}

		default:
			resp.Result = &rpc.OpenFileResponse_Error{
				Error: &rpc.Error{
					Err: &rpc.Error_Msg{Msg: err.Error()},
				},
			}

		case nil:
			file.count++
			resp.Result = &rpc.OpenFileResponse_Attr{
				Attr: tfs.CreateAttr(info),
			}
		}

		return
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	switch err {
	case os.ErrNotExist:
		resp.Result = &rpc.OpenFileResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)},
			},
		}
		return

	case os.ErrPermission:
		resp.Result = &rpc.OpenFileResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Errno{Errno: uint32(syscall.EPERM)},
			},
		}
		return

	default:
		resp.Result = &rpc.OpenFileResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Msg{Msg: err.Error()},
			},
		}
		return

	case nil:
	}

	s.files[path] = &file{
		f:     f,
		count: 1,
	}

	info, err := f.Stat()
	if err != nil {
		resp.Result = &rpc.OpenFileResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Msg{Msg: err.Error()},
			},
		}
		return
	}

	resp.Result = &rpc.OpenFileResponse_Attr{
		Attr: tfs.CreateAttr(info),
	}

	return
}

func (s *Server) Allocate(ctx context.Context, req *rpc.AllocateRequest) (resp *rpc.AllocateResponse, errRet error) {
	resp = new(rpc.AllocateResponse)

	file, ok := s.files[filepath.Join(s.Root, req.Path)]
	if !ok {
		resp.Error = &rpc.Error{
			Err: &rpc.Error_Errno{Errno: uint32(syscall.EBADF)},
		}
	}

	if err := file.f.Truncate(int64(req.Offset + req.Size)); err != nil {
		resp.Error = &rpc.Error{
			Err: &rpc.Error_Msg{Msg: err.Error()},
		}
	}

	return
}

func (s *Server) ReadFile(req *rpc.ReadFileRequest, respStream rpc.Tfs_ReadFileServer) (errRet error) {
	path := filepath.Join(s.Root, req.Path)
	file, ok := s.files[path]
	if !ok {
		resp := &rpc.ReadFileResponse{
			Result: &rpc.ReadFileResponse_Error{
				Error: &rpc.Error{
					Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)},
				},
			},
		}
		if err := respStream.Send(resp); err != nil {
			errRet = errors.Wrap(err, "send not exist response failed")
		}
		return
	}

	offset := req.Offset
	totalSize := req.Size

	buf := make([]byte, tfs.BufSize)
	for {
		select {
		case <-respStream.Context().Done():
			return respStream.Context().Err()

		default:
		}

		if totalSize > tfs.BufSize {
			n, err := file.f.ReadAt(buf, int64(offset))
			switch err {
			default:
				resp := &rpc.ReadFileResponse{
					Result: &rpc.ReadFileResponse_Error{
						Error: &rpc.Error{
							Err: &rpc.Error_Msg{Msg: err.Error()},
						},
					},
				}
				if err := respStream.Send(resp); err != nil {
					errRet = errors.Wrap(err, "send read error response failed")
				}
				return

			case io.EOF:
				resp := &rpc.ReadFileResponse{
					Result: &rpc.ReadFileResponse_Data{
						Data: buf[:n], // reduce allocate, last send use exists buf
					},
				}
				if err := respStream.Send(resp); err != nil {
					errRet = errors.Wrap(err, "send last data response failed")
				}
				return

			case nil:
				resp := &rpc.ReadFileResponse{
					Result: &rpc.ReadFileResponse_Data{
						Data: make([]byte, tfs.BufSize),
					},
				}
				copy(resp.Result.(*rpc.ReadFileResponse_Data).Data, buf)

				if err := respStream.Send(resp); err != nil {
					errRet = errors.Wrap(err, "send data response failed")
				}

				totalSize -= tfs.BufSize
				offset += tfs.BufSize
			}
		} else {
			buf = buf[:totalSize]
			n, err := file.f.ReadAt(buf, int64(offset))
			switch err {
			default:
				resp := &rpc.ReadFileResponse{
					Result: &rpc.ReadFileResponse_Error{
						Error: &rpc.Error{
							Err: &rpc.Error_Msg{Msg: err.Error()},
						},
					},
				}
				if err := respStream.Send(resp); err != nil {
					errRet = errors.Wrap(err, "send read error response failed")
				}
				return

			case io.EOF, nil:
				resp := &rpc.ReadFileResponse{
					Result: &rpc.ReadFileResponse_Data{
						Data: buf[:n], // reduce allocate, last send use exists buf
					},
				}
				if err := respStream.Send(resp); err != nil {
					errRet = errors.Wrap(err, "send last data response failed")
				}
				return
			}
		}
	}
}

func (s *Server) WriteFile(reqStream rpc.Tfs_WriteFileServer) error {
	var (
		file    *file
		written uint64
	)

	for {
		select {
		case <-reqStream.Context().Done():
			return reqStream.Context().Err()

		default:
		}

		req, err := reqStream.Recv()
		switch err {
		case io.EOF:
			if err := reqStream.SendAndClose(&rpc.WriteFileResponse{
				Result: &rpc.WriteFileResponse_Written{
					Written: written,
				},
			}); err != nil {
				return errors.Wrap(err, "send written response failed")
			}
			return nil

		default:
			return errors.Wrap(err, "recv write req failed")

		case nil:
		}

		if file == nil {
			path := filepath.Join(s.Root, req.Path)
			f, ok := s.files[path]
			if !ok {
				if err := reqStream.SendAndClose(&rpc.WriteFileResponse{
					Result: &rpc.WriteFileResponse_Error{
						Error: &rpc.Error{
							Err: &rpc.Error_Errno{Errno: uint32(syscall.EBADF)},
						},
					},
				}); err != nil {
					return errors.Wrap(err, "send bad descriptor response failed")
				}
			}
			file = f
		}

		if _, err := file.f.WriteAt(req.Data, int64(req.Offset)); err != nil {
			if err := reqStream.SendAndClose(&rpc.WriteFileResponse{
				Result: &rpc.WriteFileResponse_Error{
					Error: &rpc.Error{
						Err: &rpc.Error_Msg{Msg: err.Error()},
					},
				},
			}); err != nil {
				return errors.Wrap(err, "send write error response failed")
			}
		}

		written += uint64(len(req.Data))
	}
}

func (s *Server) CloseFile(ctx context.Context, req *rpc.CloseFileRequest) (resp *rpc.CloseFileResponse, err error) {
	resp = new(rpc.CloseFileResponse)

	path := filepath.Join(s.Root, req.Path)
	file, ok := s.files[path]
	if !ok {
		return
	}

	file.count--
	if file.count == 0 {
		delete(s.files, path)
	}
	return
}

func (s *Server) SyncFile(reqStream rpc.Tfs_SyncFileServer) error {
	if err := reqStream.SendAndClose(&rpc.SyncFileResponse{
		Error: &rpc.Error{
			Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOTSUP)},
		},
	}); err != nil {
		return errors.Wrap(err, "send not supported response failed")
	}
	return nil
}

func (s *Server) GetAttr(ctx context.Context, req *rpc.GetAttrRequest) (resp *rpc.GetAttrResponse, errRet error) {
	resp = new(rpc.GetAttrResponse)

	path := filepath.Join(s.Root, req.Path)
	info, err := os.Stat(path)
	switch err {
	case os.ErrNotExist:
		resp.Result = &rpc.GetAttrResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)},
			},
		}

	case os.ErrPermission:
		resp.Result = &rpc.GetAttrResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Errno{Errno: uint32(syscall.EPERM)},
			},
		}

	default:
		resp.Result = &rpc.GetAttrResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Msg{Msg: err.Error()},
			},
		}

	case nil:
		resp.Result = &rpc.GetAttrResponse_Attr{
			Attr: tfs.CreateAttr(info),
		}
	}

	return
}

func (s *Server) SetAttr(ctx context.Context, req *rpc.SetAttrRequest) (resp *rpc.SetAttrResponse, errRet error) {
	resp = new(rpc.SetAttrResponse)

	path := filepath.Join(s.Root, req.Path)
	_, err := os.Stat(path)
	switch err {
	case os.ErrNotExist:
		resp.Result = &rpc.SetAttrResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)},
			},
		}
		return

	case os.ErrPermission:
		resp.Result = &rpc.SetAttrResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Errno{Errno: uint32(syscall.EPERM)},
			},
		}
		return

	default:
		resp.Result = &rpc.SetAttrResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Msg{Msg: err.Error()},
			},
		}
		return

	case nil:
	}

	attr := req.Attr

	if err := os.Truncate(path, int64(attr.Size)); err != nil {
		resp.Result = &rpc.SetAttrResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Msg{Msg: err.Error()},
			},
		}
		return
	}

	if err := os.Chmod(path, os.FileMode(attr.Mode)); err != nil {
		resp.Result = &rpc.SetAttrResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Msg{Msg: err.Error()},
			},
		}
		return
	}

	// not support change atime, mtime, ctime

	info, err := os.Stat(path)
	if err != nil {
		resp.Result = &rpc.SetAttrResponse_Error{
			Error: &rpc.Error{
				Err: &rpc.Error_Msg{Msg: err.Error()},
			},
		}
		return
	}

	resp.Result = &rpc.SetAttrResponse_Attr{
		Attr: tfs.CreateAttr(info),
	}

	return
}

func rmFileOrDir(path string) *rpc.Error {
	err := os.Remove(path)

	switch err {
	case os.ErrNotExist:
		return &rpc.Error{
			Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)},
		}

	case os.ErrPermission:
		return &rpc.Error{
			Err: &rpc.Error_Errno{Errno: uint32(syscall.EPERM)},
		}

	case nil:
		return nil
	}

	if pathErr, ok := err.(*os.PathError); ok {
		if pathErr.Err == syscall.ENOTEMPTY {
			return &rpc.Error{
				Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOTEMPTY)},
			}
		}
	}

	return &rpc.Error{
		Err: &rpc.Error_Msg{Msg: err.Error()},
	}
}
