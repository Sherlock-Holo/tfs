package server

import (
	"context"
	"io"
	"os"
	"syscall"

	"github.com/Sherlock-Holo/tfs/api/rpc"
	"github.com/Sherlock-Holo/tfs/internal"
	"github.com/Sherlock-Holo/tfs/internal/server/fs"
	"github.com/golang/protobuf/ptypes/empty"
	errors "golang.org/x/xerrors"
)

type Server struct {
	allocateCh   chan<- fs.AllocateRequest
	readCh       chan<- fs.ReadRequest
	writeCh      chan<- fs.WriteRequest
	attrCh       chan<- fs.AttrRequest
	createCh     chan<- fs.CreateRequest
	mkdirCh      chan<- fs.MkdirRequest
	deleteFileCh chan<- fs.DeleteFileRequest
	rmdirCh      chan<- fs.RmdirRequest
	readDirCh    chan<- fs.ReadDirRequest
	lookupCh     chan<- fs.LookupRequest
	renameCh     chan<- fs.RenameRequest
	openFileCh   chan<- fs.OpenFileRequest
}

func NewServer(
	allocateCh chan<- fs.AllocateRequest,
	readCh chan<- fs.ReadRequest,
	writeCh chan<- fs.WriteRequest,
	attrCh chan<- fs.AttrRequest,
	createCh chan<- fs.CreateRequest,
	mkdirCh chan<- fs.MkdirRequest,
	deleteFileCh chan<- fs.DeleteFileRequest,
	rmdirCh chan<- fs.RmdirRequest,
	readDirCh chan<- fs.ReadDirRequest,
	lookupCh chan<- fs.LookupRequest,
	renameCh chan<- fs.RenameRequest,
	openFileCh chan<- fs.OpenFileRequest,
) *Server {
	return &Server{
		allocateCh:   allocateCh,
		readCh:       readCh,
		writeCh:      writeCh,
		attrCh:       attrCh,
		createCh:     createCh,
		mkdirCh:      mkdirCh,
		deleteFileCh: deleteFileCh,
		rmdirCh:      rmdirCh,
		readDirCh:    readDirCh,
		lookupCh:     lookupCh,
		renameCh:     renameCh,
		openFileCh:   openFileCh,
	}
}

func (s *Server) Nothing(ctx context.Context, _ *empty.Empty) (*empty.Empty, error) {
	return new(empty.Empty), nil
}

func (s *Server) ReadDir(ctx context.Context, req *rpc.ReadDirRequest) (resp *rpc.ReadDirResponse, errRet error) {
	resp = new(rpc.ReadDirResponse)

	respCh := make(chan fs.ReadDirResponse, 1)

	fsReq := fs.ReadDirRequest{
		Ctx:    ctx,
		Name:   req.DirPath,
		RespCh: respCh,
	}

	select {
	case <-ctx.Done():
		resp.Error = &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}
		return

	case s.readDirCh <- fsReq:
	}

	select {
	case <-ctx.Done():
		resp.Error = &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}

	case fsResp := <-respCh:
		err := fsResp.Err
		switch {
		default:
			resp.Error = &rpc.Error{Err: &rpc.Error_Msg{Msg: err.Error()}}

		case errors.Is(err, os.ErrNotExist):
			resp.Error = &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)}}

		case err == nil:
			resp.DirEntries = fsResp.Entries
		}
	}

	return
}

func (s *Server) Lookup(ctx context.Context, req *rpc.LookupRequest) (resp *rpc.LookupResponse, errRet error) {
	resp = new(rpc.LookupResponse)

	respCh := make(chan fs.LookupResponse, 1)

	fsReq := fs.LookupRequest{
		Ctx:     ctx,
		DirPath: req.DirPath,
		Name:    req.Filename,
		RespCh:  respCh,
	}

	select {
	case <-ctx.Done():
		resp.Result = &rpc.LookupResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}}
		return

	case s.lookupCh <- fsReq:
	}

	select {
	case <-ctx.Done():
		resp.Result = &rpc.LookupResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}}

	case fsResp := <-respCh:
		err := fsResp.Err
		switch {
		default:
			resp.Result = &rpc.LookupResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Msg{Msg: err.Error()}}}

		case errors.Is(err, os.ErrNotExist):
			resp.Result = &rpc.LookupResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)}}}

		case err == nil:
			resp.Result = &rpc.LookupResponse_Attr{Attr: fsResp.Attr}
		}
	}

	return
}

func (s *Server) Mkdir(ctx context.Context, req *rpc.MkdirRequest) (resp *rpc.MkdirResponse, errRet error) {
	resp = new(rpc.MkdirResponse)

	respCh := make(chan fs.MkdirResponse, 1)

	fsReq := fs.MkdirRequest{
		Ctx:     ctx,
		DirPath: req.DirPath,
		Name:    req.NewName,
		RespCh:  respCh,
	}

	select {
	case <-ctx.Done():
		resp.Result = &rpc.MkdirResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}}
		return

	case s.mkdirCh <- fsReq:
	}

	select {
	case <-ctx.Done():
		resp.Result = &rpc.MkdirResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}}

	case fsResp := <-respCh:
		err := fsResp.Err
		switch {
		default:
			resp.Result = &rpc.MkdirResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Msg{Msg: err.Error()}}}

		case errors.Is(err, os.ErrNotExist):
			resp.Result = &rpc.MkdirResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)}}}

		case err == nil:
			resp.Result = &rpc.MkdirResponse_Attr{Attr: fsResp.Attr}
		}
	}

	return
}

func (s *Server) CreateFile(ctx context.Context, req *rpc.CreateFileRequest) (resp *rpc.CreateFileResponse, errRet error) {
	resp = new(rpc.CreateFileResponse)

	respCh := make(chan fs.CreateResponse, 1)

	fsReq := fs.CreateRequest{
		Ctx:     ctx,
		DirPath: req.DirPath,
		Name:    req.Filename,
		RespCh:  respCh,
	}

	select {
	case <-ctx.Done():
		resp.Result = &rpc.CreateFileResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}}
		return

	case s.createCh <- fsReq:
	}

	select {
	case <-ctx.Done():
		resp.Result = &rpc.CreateFileResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}}

	case fsResp := <-respCh:
		err := fsResp.Err
		switch {
		default:
			resp.Result = &rpc.CreateFileResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Msg{Msg: err.Error()}}}

		case errors.Is(err, os.ErrNotExist):
			resp.Result = &rpc.CreateFileResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)}}}

		case err == nil:
			resp.Result = &rpc.CreateFileResponse_Attr{Attr: fsResp.Attr}
		}
	}

	return
}

func (s *Server) Unlink(ctx context.Context, req *rpc.UnlinkRequest) (resp *rpc.UnlinkResponse, errRet error) {
	resp = new(rpc.UnlinkResponse)

	respCh := make(chan error, 1)
	fsReq := fs.DeleteFileRequest{
		Ctx:     ctx,
		DirPath: req.DirPath,
		Name:    req.Filename,
		RespCh:  respCh,
	}

	select {
	case <-ctx.Done():
		resp.Error = &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}
		return

	case s.deleteFileCh <- fsReq:
	}

	select {
	case <-ctx.Done():
		resp.Error = &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}

	case err := <-respCh:
		switch {
		default:
			resp.Error = &rpc.Error{Err: &rpc.Error_Msg{Msg: err.Error()}}

		case errors.Is(err, os.ErrNotExist):
			resp.Error = &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)}}

		case err == nil:
		}
	}

	return
}

func (s *Server) RmDir(ctx context.Context, req *rpc.RmDirRequest) (resp *rpc.RmDirResponse, errRet error) {
	resp = new(rpc.RmDirResponse)

	respCh := make(chan error, 1)
	fsReq := fs.RmdirRequest{
		Ctx:     ctx,
		DirPath: req.DirPath,
		Name:    req.RmName,
		RespCh:  respCh,
	}

	select {
	case <-ctx.Done():
		resp.Error = &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}
		return

	case s.rmdirCh <- fsReq:
	}

	select {
	case <-ctx.Done():
		resp.Error = &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}

	case err := <-respCh:
		switch {
		default:
			resp.Error = &rpc.Error{Err: &rpc.Error_Msg{Msg: err.Error()}}

		case errors.Is(err, os.ErrNotExist):
			resp.Error = &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)}}

		case err == nil:
		}
	}

	return
}

func (s *Server) Rename(ctx context.Context, req *rpc.RenameRequest) (resp *rpc.RenameResponse, errRet error) {
	resp = new(rpc.RenameResponse)

	respCh := make(chan error, 1)
	fsReq := fs.RenameRequest{
		Ctx:        ctx,
		OldDirPath: req.DirPath,
		OldName:    req.OldName,
		NewDirPath: req.NewDirPath,
		NewName:    req.NewName,
		RespCh:     respCh,
	}

	select {
	case <-ctx.Done():
		resp.Error = &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}
		return

	case s.renameCh <- fsReq:
	}

	select {
	case <-ctx.Done():
		resp.Error = &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}

	case err := <-respCh:
		switch {
		default:
			resp.Error = &rpc.Error{Err: &rpc.Error_Msg{Msg: err.Error()}}

		case errors.Is(err, os.ErrExist):
			resp.Error = &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.EEXIST)}}

		case errors.Is(err, os.ErrNotExist):
			resp.Error = &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)}}

		case err == nil:
		}
	}

	return
}

func (s *Server) OpenFile(ctx context.Context, req *rpc.OpenFileRequest) (resp *rpc.OpenFileResponse, errRet error) {
	resp = new(rpc.OpenFileResponse)

	respCh := make(chan fs.OpenFileResponse, 1)
	fsReq := fs.OpenFileRequest{
		Ctx:    ctx,
		Name:   req.Path,
		RespCh: respCh,
	}

	select {
	case <-ctx.Done():
		resp.Result = &rpc.OpenFileResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}}
		return

	case s.openFileCh <- fsReq:
	}

	select {
	case <-ctx.Done():
		resp.Result = &rpc.OpenFileResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}}

	case fsResp := <-respCh:
		err := fsResp.Err
		switch {
		default:
			resp.Result = &rpc.OpenFileResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Msg{Msg: err.Error()}}}

		case errors.Is(err, os.ErrNotExist):
			resp.Result = &rpc.OpenFileResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)}}}

		case err == nil:
			resp.Result = &rpc.OpenFileResponse_Attr{Attr: fsResp.Attr}
		}
	}

	return
}

func (s *Server) Allocate(ctx context.Context, req *rpc.AllocateRequest) (resp *rpc.AllocateResponse, errRet error) {
	resp = new(rpc.AllocateResponse)

	respCh := make(chan error, 1)
	fsReq := fs.AllocateRequest{
		Ctx:    ctx,
		Name:   req.Path,
		Offset: req.Offset,
		Size:   req.Size,
		RespCh: respCh,
	}

	select {
	case <-ctx.Done():
		resp.Error = &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}
		return

	case s.allocateCh <- fsReq:
	}

	select {
	case <-ctx.Done():
		resp.Error = &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}

	case err := <-respCh:
		switch {
		default:
			resp.Error = &rpc.Error{Err: &rpc.Error_Msg{Msg: err.Error()}}

		case errors.Is(err, os.ErrNotExist):
			resp.Error = &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)}}

		case err == nil:
		}
	}

	return
}

func (s *Server) ReadFile(req *rpc.ReadFileRequest, respStream rpc.Tfs_ReadFileServer) (errRet error) {
	ctx := respStream.Context()
	respCh := make(chan fs.ReadResponse, 1)
	fsReq := fs.ReadRequest{
		Ctx:    ctx,
		Name:   req.Path,
		Offset: req.Offset,
		Size:   req.Size,
		RespCh: respCh,
	}

	select {
	case <-ctx.Done():
		if err := respStream.Send(&rpc.ReadFileResponse{
			Result: &rpc.ReadFileResponse_Error{
				Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}},
			},
		}); err != nil {
			return errors.Errorf("send read file timeout response failed: %w", err)
		}

	case s.readCh <- fsReq:
	}

	select {
	case <-ctx.Done():
		if err := respStream.Send(&rpc.ReadFileResponse{
			Result: &rpc.ReadFileResponse_Error{
				Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}},
			},
		}); err != nil {
			return errors.Errorf("send read file timeout response failed: %w", err)
		}

	case fsResp := <-respCh:
		err := fsResp.Err
		switch {
		default:
			if err := respStream.Send(&rpc.ReadFileResponse{
				Result: &rpc.ReadFileResponse_Error{
					Error: &rpc.Error{Err: &rpc.Error_Msg{Msg: err.Error()}},
				},
			}); err != nil {
				return errors.Errorf("send read file error response failed: %w", err)
			}

		case errors.Is(err, os.ErrNotExist):
			if err := respStream.Send(&rpc.ReadFileResponse{
				Result: &rpc.ReadFileResponse_Error{
					Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)}},
				},
			}); err != nil {
				return errors.Errorf("send read file error response failed: %w", err)
			}

		case err == nil:
		}

		for _, data := range internal.ChunkData(fsResp.Data) {
			if err := respStream.Send(&rpc.ReadFileResponse{
				Result: &rpc.ReadFileResponse_Data{Data: data},
			}); err != nil {
				return errors.Errorf("send read file response failed: %w", err)
			}
		}
	}

	return
}

func (s *Server) WriteFile(reqStream rpc.Tfs_WriteFileServer) error {
	ctx := reqStream.Context()
	respCh := make(chan fs.WriteResponse, 1)
	var written uint64
	for {
		req, err := reqStream.Recv()
		switch {
		default:
			return errors.Errorf("recv write file request failed: %w", err)

		case errors.Is(err, io.EOF):
			if err := reqStream.SendAndClose(&rpc.WriteFileResponse{
				Result: &rpc.WriteFileResponse_Written{Written: written},
			}); err != nil {
				return errors.Errorf("send write file response failed: %w", err)
			}
			return nil

		case err == nil:
		}

		fsReq := fs.WriteRequest{
			Ctx:    ctx,
			Name:   req.Path,
			Offset: req.Offset,
			Data:   req.Data,
			RespCh: respCh,
		}

		select {
		case <-ctx.Done():
			if err := reqStream.SendAndClose(&rpc.WriteFileResponse{
				Result: &rpc.WriteFileResponse_Error{
					Error: &rpc.Error{
						Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)},
					},
				},
			}); err != nil {
				return errors.Errorf("send write file timeout response failed: %w", err)
			}

		case s.writeCh <- fsReq:
		}

		select {
		case <-ctx.Done():
			if err := reqStream.SendAndClose(&rpc.WriteFileResponse{
				Result: &rpc.WriteFileResponse_Error{
					Error: &rpc.Error{
						Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)},
					},
				},
			}); err != nil {
				return errors.Errorf("send write file timeout response failed: %w", err)
			}

		case fsResp := <-respCh:
			err := fsResp.Err
			switch {
			default:
				if err := reqStream.SendAndClose(&rpc.WriteFileResponse{
					Result: &rpc.WriteFileResponse_Error{
						Error: &rpc.Error{
							Err: &rpc.Error_Msg{Msg: err.Error()},
						},
					},
				}); err != nil {
					return errors.Errorf("send write file error response failed: %w", err)
				}

			case errors.Is(err, os.ErrNotExist):
				if err := reqStream.SendAndClose(&rpc.WriteFileResponse{
					Result: &rpc.WriteFileResponse_Error{
						Error: &rpc.Error{
							Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)},
						},
					},
				}); err != nil {
					return errors.Errorf("send write file timeout response failed: %w", err)
				}

			case err == nil:
				written += uint64(fsResp.Written)
			}
		}
	}
}

func (s *Server) CloseFile(ctx context.Context, req *rpc.CloseFileRequest) (resp *rpc.CloseFileResponse, err error) {
	resp = new(rpc.CloseFileResponse)
	return
}

func (s *Server) SyncFile(reqStream rpc.Tfs_SyncFileServer) error {
	return nil
}

func (s *Server) GetAttr(ctx context.Context, req *rpc.GetAttrRequest) (resp *rpc.GetAttrResponse, errRet error) {
	resp = new(rpc.GetAttrResponse)

	respCh := make(chan fs.AttrResponse, 1)
	fsReq := fs.AttrRequest{
		Ctx:    ctx,
		Name:   req.Path,
		Op:     fs.AttrGet,
		RespCh: respCh,
	}

	select {
	case <-ctx.Done():
		resp.Result = &rpc.GetAttrResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}}
		return

	case s.attrCh <- fsReq:
	}

	select {
	case <-ctx.Done():
		resp.Result = &rpc.GetAttrResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}}

	case fsResp := <-respCh:
		resp.Result = &rpc.GetAttrResponse_Attr{Attr: fsResp.Attr}
	}

	return
}

func (s *Server) SetAttr(ctx context.Context, req *rpc.SetAttrRequest) (resp *rpc.SetAttrResponse, errRet error) {
	resp = new(rpc.SetAttrResponse)

	respCh := make(chan fs.AttrResponse, 1)
	fsReq := fs.AttrRequest{
		Ctx:    ctx,
		Name:   req.Path,
		Op:     fs.AttrSet,
		Attr:   req.Attr,
		RespCh: respCh,
	}

	select {
	case <-ctx.Done():
		resp.Result = &rpc.SetAttrResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}}
		return

	case s.attrCh <- fsReq:
	}

	select {
	case <-ctx.Done():
		resp.Result = &rpc.SetAttrResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ETIME)}}}

	case fsResp := <-respCh:
		err := fsResp.Err
		switch {
		default:
			resp.Result = &rpc.SetAttrResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Msg{Msg: err.Error()}}}

		case errors.Is(err, os.ErrNotExist):
			resp.Result = &rpc.SetAttrResponse_Error{Error: &rpc.Error{Err: &rpc.Error_Errno{Errno: uint32(syscall.ENOENT)}}}

		case err == nil:
			resp.Result = &rpc.SetAttrResponse_Attr{Attr: fsResp.Attr}
		}
	}

	return
}
