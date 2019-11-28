package fs

import (
	"context"
	"os"

	"github.com/Sherlock-Holo/tfs/api/rpc"
)

type AllocateRequest struct {
	Ctx    context.Context
	Name   string
	Offset uint64
	Size   uint64
	RespCh chan<- error
}

type ReadRequest struct {
	Ctx    context.Context
	Name   string
	Offset uint64
	Size   uint64
	RespCh chan<- ReadResponse
}

type ReadResponse struct {
	Offset uint64
	Data   []byte
	Err    error
}

type WriteRequest struct {
	Ctx    context.Context
	Name   string
	Offset uint64
	Data   []byte
	RespCh chan<- WriteResponse
}

type WriteResponse struct {
	Written int
	Err     error
}

type AttrOp int

const (
	AttrSet AttrOp = iota + 1
	AttrGet
)

type AttrRequest struct {
	Ctx    context.Context
	Name   string
	Op     AttrOp
	Attr   *rpc.Attr
	RespCh chan<- AttrResponse
}

type AttrResponse struct {
	Attr *rpc.Attr
	Err  error
}

type CreateRequest struct {
	Ctx     context.Context
	DirPath string
	Name    string
	Mode    uint32
	RespCh  chan<- CreateResponse
}

type CreateResponse struct {
	Attr *rpc.Attr
	Err  error
}

type MkdirRequest struct {
	Ctx     context.Context
	DirPath string
	Name    string
	Mode    os.FileMode
	RespCh  chan<- MkdirResponse
}

type MkdirResponse struct {
	Attr *rpc.Attr
	Err  error
}

type DeleteFileRequest struct {
	Ctx     context.Context
	DirPath string
	Name    string
	RespCh  chan<- error
}

type RmdirRequest struct {
	Ctx     context.Context
	DirPath string
	Name    string
	RespCh  chan<- error
}

type ReadDirRequest struct {
	Ctx    context.Context
	Name   string
	RespCh chan<- ReadDirResponse
}

type ReadDirResponse struct {
	Entries []*rpc.ReadDirResponse_DirEntry
	Err     error
}

type LookupRequest struct {
	Ctx     context.Context
	DirPath string
	Name    string
	RespCh  chan<- LookupResponse
}

type LookupResponse struct {
	Attr *rpc.Attr
	Err  error
}

type RenameRequest struct {
	Ctx        context.Context
	OldDirPath string
	OldName    string
	NewDirPath string
	NewName    string
	RespCh     chan error
}

type OpenFileRequest struct {
	Ctx    context.Context
	Name   string
	RespCh chan OpenFileResponse
}

type OpenFileResponse struct {
	Attr *rpc.Attr
	Err  error
}

type fileTimeoutCloseRequest struct {
	name string
}
