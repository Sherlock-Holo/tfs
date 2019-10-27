package fs

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/Sherlock-Holo/tfs/api/rpc"
	"github.com/Sherlock-Holo/tfs/internal"
	log "github.com/sirupsen/logrus"
	errors "golang.org/x/xerrors"
)

type Tree struct {
	shutdown     context.Context
	shutdownFunc context.CancelFunc

	root  string
	files map[string]*File

	allocateCh   chan AllocateRequest
	readCh       chan ReadRequest
	writeCh      chan WriteRequest
	attrCh       chan AttrRequest
	createCh     chan CreateRequest
	mkdirCh      chan MkdirRequest
	deleteFileCh chan DeleteFileRequest
	rmdirCh      chan RmdirRequest
	readDirCh    chan ReadDirRequest
	lookupCh     chan LookupRequest
	renameCh     chan RenameRequest
	openFileCh   chan OpenFileRequest

	fileTimeoutCloseCh chan fileTimeoutCloseRequest
}

func NewTree(root string) *Tree {
	ctx, cancel := context.WithCancel(context.Background())

	return &Tree{
		shutdown:     ctx,
		shutdownFunc: cancel,

		root:  root,
		files: make(map[string]*File),

		allocateCh:   make(chan AllocateRequest, 1),
		readCh:       make(chan ReadRequest, 1),
		writeCh:      make(chan WriteRequest, 1),
		attrCh:       make(chan AttrRequest, 1),
		createCh:     make(chan CreateRequest, 1),
		mkdirCh:      make(chan MkdirRequest, 1),
		deleteFileCh: make(chan DeleteFileRequest, 1),
		rmdirCh:      make(chan RmdirRequest, 1),
		readDirCh:    make(chan ReadDirRequest, 1),
		lookupCh:     make(chan LookupRequest, 1),
		renameCh:     make(chan RenameRequest, 1),
		openFileCh:   make(chan OpenFileRequest, 1),

		fileTimeoutCloseCh: make(chan fileTimeoutCloseRequest, 1),
	}
}

func (t *Tree) Run() {
	go t.handleLoop()
}

func (t *Tree) Shutdown() {
	t.shutdownFunc()
}

func (t *Tree) AllocateCh() chan<- AllocateRequest {
	return t.allocateCh
}

func (t *Tree) ReadCh() chan<- ReadRequest {
	return t.readCh
}

func (t *Tree) WriteCh() chan<- WriteRequest {
	return t.writeCh
}

func (t *Tree) AttrCh() chan<- AttrRequest {
	return t.attrCh
}

func (t *Tree) CreateCh() chan<- CreateRequest {
	return t.createCh
}

func (t *Tree) MkdirCh() chan<- MkdirRequest {
	return t.mkdirCh
}

func (t *Tree) DeleteFileCh() chan<- DeleteFileRequest {
	return t.deleteFileCh
}

func (t *Tree) RmdirCh() chan<- RmdirRequest {
	return t.rmdirCh
}

func (t *Tree) ReadDirCh() chan<- ReadDirRequest {
	return t.readDirCh
}

func (t *Tree) LookupCh() chan<- LookupRequest {
	return t.lookupCh
}

func (t *Tree) RenameCh() chan<- RenameRequest {
	return t.renameCh
}

func (t *Tree) OpenFileCh() chan<- OpenFileRequest {
	return t.openFileCh
}

// handleLoop
// when attrReq receive, user want to set or get file/directory attr.
// when createReq receive, user want to create a file.
// when mkdirReq receive, user want to make a directory.
// when allocateReq receive, user want to allocate a file.
// when readReq receive, user want to read data from a file.
// when writeReq receive, user want to write data to a file.
// when deleteFileReq receive, user want to delete a file.
// when readDirReq receive, user want to read all entries in a directory.
// when rmdirReq receive, user want to remove a directory.
// when fileTimeoutReq receive, a file has no access for a long time,
// close it to reduce resource.
func (t *Tree) handleLoop() {
	for {
		select {
		case <-t.shutdown.Done():
			log.Info("fs tree shutdown...")
			return

		case attrReq := <-t.attrCh:
			t.doAttrReq(attrReq)

		case createReq := <-t.createCh:
			t.doCreateReq(createReq)

		case mkdirReq := <-t.mkdirCh:
			t.doMkdirReq(mkdirReq)

		case allocateReq := <-t.allocateCh:
			t.doAllocateReq(allocateReq)

		case readReq := <-t.readCh:
			t.doReadReq(readReq)

		case writeReq := <-t.writeCh:
			t.doWriteReq(writeReq)

		case deleteFileReq := <-t.deleteFileCh:
			t.doDeleteFileReq(deleteFileReq)

		case rmdirReq := <-t.rmdirCh:
			t.doRmdirReq(rmdirReq)

		case readDirReq := <-t.readDirCh:
			t.doReadDirReq(readDirReq)

		case lookupReq := <-t.lookupCh:
			t.doLookupReq(lookupReq)

		case renameReq := <-t.renameCh:
			t.doRenameReq(renameReq)

		case openFileReq := <-t.openFileCh:
			t.doOpenFileReq(openFileReq)

		case fileTimeoutCloseReq := <-t.fileTimeoutCloseCh:
			// file timeout, close to reduce resource
			delete(t.files, fileTimeoutCloseReq.name)
		}
	}
}

func (t *Tree) doAttrReq(req AttrRequest) {
	path := filepath.Join(t.root, req.Name)
	ctx := req.Ctx
	respCh := req.RespCh

	var resp AttrResponse

	switch req.Op {
	case AttrGet:
		info, err := os.Stat(path)
		switch err {
		default:
			resp.Err = errors.Errorf("get %s attr failed: %w", path, err)

		case nil:
			resp.Attr = internal.CreateAttr(info)
		}

		select {
		case <-ctx.Done():
			log.Warnf("%+v", errors.Errorf("get file %s attr cancel or timeout: %w", path, err))

		case respCh <- resp:
		}

	case AttrSet:
		if err := setAttr(path, req.Attr); err != nil {
			resp.Err = errors.Errorf("set file %s attr failed: %w", path, err)

			select {
			case <-ctx.Done():
				log.Warnf("%+v", errors.Errorf("set file %s attr cancel or timeout: %w", path, err))

			case respCh <- resp:
			}

			return
		}

		info, err := os.Stat(path)
		if err != nil {
			resp.Err = errors.Errorf("get file %s attr failed: %w", path, err)

			select {
			case <-ctx.Done():
				log.Warnf("%+v", errors.Errorf("set file %s attr cancel or timeout: %w", path, err))

			case respCh <- resp:
			}

			return
		}

		resp.Attr = internal.CreateAttr(info)

		select {
		case <-ctx.Done():
			log.Warnf("%+v", errors.Errorf("set file %s attr cancel or timeout: %w", path, err))

		case respCh <- resp:
		}
	}
}

func (t *Tree) doCreateReq(req CreateRequest) {
	path := filepath.Join(t.root, req.DirPath, req.Name)
	mode := req.Mode
	ctx := req.Ctx
	respCh := req.RespCh

	var resp CreateResponse

	if _, ok := t.files[path]; ok {
		resp.Err = errors.Errorf("create file %s mode %d failed: %w", path, mode, os.ErrExist)
		select {
		case <-ctx.Done():
			log.Warnf("%+v", errors.Errorf("create file %s mode %d cancel or timeout: %w", path, mode, ctx.Err()))

		case respCh <- resp:
		}

		return
	}

	_, err := os.Stat(path)
	switch {
	default:
		resp.Err = errors.Errorf("create file %s mode %d failed: %w", path, mode, err)

	case err == nil:
		resp.Err = errors.Errorf("create file %s mode %d failed: %w", path, mode, os.ErrExist)

	case errors.Is(err, os.ErrNotExist):
		var f *os.File
		f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.FileMode(mode))
		switch {
		default:
			resp.Err = errors.Errorf("create file %s mode %d failed: %w", path, mode, err)

		case err == nil:
			t.files[path] = newFile(f, t.fileTimeoutCloseCh)

			info, err := f.Stat()
			if err != nil {
				resp.Err = errors.Errorf("create file %s mode %d failed: %w", path, mode, err)
			} else {
				resp.Attr = internal.CreateAttr(info)
			}
		}
	}

	respCh <- resp
}

func (t *Tree) doMkdirReq(req MkdirRequest) {
	path := filepath.Join(t.root, req.DirPath, req.Name)
	ctx := req.Ctx
	respCh := req.RespCh

	var resp MkdirResponse

	err := os.Mkdir(path, os.ModeDir|0644)
	switch {
	default:
		resp.Err = errors.Errorf("mkdir %s failed: %w", err)

	case errors.Is(err, os.ErrExist):
		resp.Err = errors.Errorf("mkdir %s failed: %w", os.ErrExist)

	case err == nil:
		info, err := os.Stat(path)
		if err != nil {
			resp.Err = errors.Errorf("mkdir %s failed: %w", err)
		} else {
			resp.Attr = internal.CreateAttr(info)
		}
	}

	select {
	case <-ctx.Done():
		log.Warnf("%+v", errors.Errorf("mkdir %s cancel or timeout: %w", ctx.Err()))

	case respCh <- resp:
	}
}

func (t *Tree) doAllocateReq(req AllocateRequest) {
	path := filepath.Join(t.root, req.Name)
	ctx := req.Ctx
	respCh := req.RespCh

	file, ok := t.files[path]
	if !ok {
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		switch {
		default:
			err = errors.Errorf("allocate file %s failed: %w", path, err)

		case errors.Is(err, os.ErrNotExist):
			err = errors.Errorf("allocate file %s failed: %w", path, os.ErrNotExist)

		case err == nil:
			file = newFile(f, t.fileTimeoutCloseCh)
		}

		if err != nil {
			select {
			case <-ctx.Done():
				log.Warnf("%+v", errors.Errorf("allocate file %s cancel or timeout: %w", path, ctx.Err()))

			case respCh <- err:
			}

			return
		}
	}

	select {
	case <-ctx.Done():
		log.Warnf("%+v", errors.Errorf("allocate file %s cancel or timeout: %w", path, ctx.Err()))

	case <-file.deleteCtx.Done():
		log.Warnf("%+v", errors.Errorf("allocate a deleted file %s: %w", path, os.ErrNotExist))

	case <-file.timeoutCtx.Done():
		go func() {
			// resend allocate request
			t.allocateCh <- req
		}()

	case file.allocateCh <- req:
	}
}

func (t *Tree) doReadReq(req ReadRequest) {
	path := filepath.Join(t.root, req.Name)
	ctx := req.Ctx
	respCh := req.RespCh

	file, ok := t.files[path]
	if !ok {
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		switch {
		default:
			err = errors.Errorf("read file %s failed: %w", path, err)

		case errors.Is(err, os.ErrNotExist):
			err = errors.Errorf("read file %s failed: %w", path, os.ErrNotExist)

		case err == nil:
			file = newFile(f, t.fileTimeoutCloseCh)
		}

		if err != nil {
			select {
			case <-ctx.Done():
				log.Warnf("%+v", errors.Errorf("read file %s cancel or timeout: %w", path, ctx.Err()))

			case respCh <- ReadResponse{Err: err}:
			}

			return
		}
	}

	select {
	case <-ctx.Done():
		log.Warnf("%+v", errors.Errorf("read file %s cancel or timeout: %w", path, ctx.Err()))

	case <-file.deleteCtx.Done():
		log.Warnf("%+v", errors.Errorf("read a deleted file %s: %w", path, os.ErrNotExist))

	case <-file.timeoutCtx.Done():
		go func() {
			// resend read request
			t.readCh <- req
		}()

	case file.readCh <- req:
	}
}

func (t *Tree) doWriteReq(req WriteRequest) {
	path := filepath.Join(t.root, req.Name)
	ctx := req.Ctx
	respCh := req.RespCh

	file, ok := t.files[path]
	if !ok {
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		switch {
		default:
			err = errors.Errorf("write file %s failed: %w", path, err)

		case errors.Is(err, os.ErrNotExist):
			err = errors.Errorf("write file %s failed: %w", path, os.ErrNotExist)

		case err == nil:
			file = newFile(f, t.fileTimeoutCloseCh)
		}

		if err != nil {
			select {
			case <-ctx.Done():
				log.Warnf("%+v", errors.Errorf("write file %s cancel or timeout: %w", path, ctx.Err()))

			case respCh <- WriteResponse{Err: err}:
			}

			return
		}
	}

	select {
	case <-ctx.Done():
		log.Warnf("%+v", errors.Errorf("write file %s cancel or timeout: %w", path, ctx.Err()))

	case <-file.deleteCtx.Done():
		log.Warnf("%+v", errors.Errorf("write a deleted file %s: %w", path, os.ErrNotExist))

	case <-file.timeoutCtx.Done():
		go func() {
			// resend write request
			t.writeCh <- req
		}()

	case file.writeCh <- req:
	}
}

func (t *Tree) doDeleteFileReq(req DeleteFileRequest) {
	path := filepath.Join(t.root, req.DirPath, req.Name)
	ctx := req.Ctx
	respCh := req.RespCh

	if file, ok := t.files[path]; ok {
		file.closeFile()
		delete(t.files, path)
	}

	err := os.Remove(path)
	switch {
	default:
		err = errors.Errorf("delete file %s failed: %w", path, err)

	case errors.Is(err, os.ErrNotExist):
		err = errors.Errorf("delete file %s failed: %w", path, os.ErrNotExist)

	case err == nil:
	}

	select {
	case <-ctx.Done():
		log.Warnf("%+v", errors.Errorf("delete file %s cancel or timeout: %w", path, ctx.Err()))

	case respCh <- err:
	}
}

func (t *Tree) doRmdirReq(req RmdirRequest) {
	path := filepath.Join(t.root, req.DirPath, req.Name)
	ctx := req.Ctx
	respCh := req.RespCh

	err := os.Remove(path)
	switch {
	default:
		err = errors.Errorf("rmdir %s failed: %w", path, err)

	case errors.Is(err, os.ErrNotExist):
		err = errors.Errorf("rmdir %s failed: %w", path, os.ErrNotExist)

	case err == nil:
	}

	select {
	case <-ctx.Done():
		log.Warnf("%+v", errors.Errorf("rmdir %s cancel or timeout: %w", path, ctx.Err()))

	case respCh <- err:
	}
}

func (t *Tree) doReadDirReq(req ReadDirRequest) {
	path := filepath.Join(t.root, req.Name)
	ctx := req.Ctx
	respCh := req.RespCh

	var resp ReadDirResponse

	infos, err := ioutil.ReadDir(path)
	switch {
	default:
		resp.Err = errors.Errorf("read dir %s failed: %w", path, err)

	case errors.Is(err, os.ErrNotExist):
		resp.Err = errors.Errorf("read dir %s failed: %w", path, os.ErrNotExist)

	case err == nil:
		for _, info := range infos {
			entry := &rpc.ReadDirResponse_DirEntry{
				Name: info.Name(),
				Mode: uint32(info.Mode()),
			}

			if info.IsDir() {
				entry.Type = rpc.EntryType_dir
			} else {
				entry.Type = rpc.EntryType_file
			}

			resp.Entries = append(resp.Entries, entry)
		}
	}

	select {
	case <-ctx.Done():
		log.Warnf("%+v", errors.Errorf("read dir %s cancel or timeout: %w", path, ctx.Err()))

	case respCh <- resp:
	}
}

func (t *Tree) doLookupReq(req LookupRequest) {
	path := filepath.Join(req.DirPath, req.Name)
	ctx := req.Ctx
	respCh := req.RespCh

	var resp LookupResponse

	info, err := os.Stat(path)
	switch {
	default:
		resp.Err = errors.Errorf("lookup %s failed: %w", path, err)

	case errors.Is(err, os.ErrNotExist):
		resp.Err = errors.Errorf("lookup %s failed: %w", path, os.ErrNotExist)

	case err == nil:
		resp.Attr = internal.CreateAttr(info)
	}

	select {
	case <-ctx.Done():
		log.Warnf("%+v", errors.Errorf("lookup %s cancel or timeout: %w", path, ctx.Err()))

	case respCh <- resp:
	}
}

func (t *Tree) doRenameReq(req RenameRequest) {
	oldPath := filepath.Join(t.root, req.OldDirPath, req.OldName)
	newPath := filepath.Join(t.root, req.NewDirPath, req.NewName)
	ctx := req.Ctx
	respCh := req.RespCh

	if _, ok := t.files[newPath]; ok {
		select {
		case <-ctx.Done():
			log.Warnf("%+v", errors.Errorf("rename old file %s to new file %s cancel or timeout: %w", oldPath, newPath, ctx.Err()))

		case respCh <- errors.Errorf("rename old file %s to new file %s failed: %w", oldPath, newPath, os.ErrExist):
		}

		return
	}

	if file, ok := t.files[oldPath]; ok {
		file.closeFile()
		delete(t.files, oldPath)
	}

	err := os.Rename(oldPath, newPath)
	if err != nil {
		err = errors.Errorf("rename old file %s to new file %s failed: %w", oldPath, newPath, err)
	}

	select {
	case <-ctx.Done():
		log.Warnf("%+v", errors.Errorf("rename old file %s to new file %s cancel or timeout: %w", oldPath, newPath, ctx.Err()))

	case respCh <- err:
	}
}

func (t *Tree) doOpenFileReq(req OpenFileRequest) {
	path := filepath.Join(t.root, req.Name)
	ctx := req.Ctx
	respCh := req.RespCh

	var (
		resp   OpenFileResponse
		file   *File
		opened bool
	)

	for {
		if file, opened = t.files[path]; opened {
			break
		}

		f, err := os.OpenFile(path, os.O_RDWR, 0)
		switch {
		default:
			resp.Err = errors.Errorf("open file %s failed: %w", path, err)

		case errors.Is(err, os.ErrNotExist):
			resp.Err = errors.Errorf("open file %s failed: %w", path, os.ErrNotExist)

		case err == nil:
			t.files[path] = newFile(f, t.fileTimeoutCloseCh)
		}
	}

	info, err := file.Stat()
	switch err {
	default:
		resp.Err = errors.Errorf("open file %s failed: %w", path, err)

	case nil:
		resp.Attr = internal.CreateAttr(info)
	}

	select {
	case <-ctx.Done():
		log.Warnf("open file %s cancel or timeout: %w", path, ctx.Err())

	case respCh <- resp:
	}
}

func setAttr(path string, attr *rpc.Attr) error {
	if attr.Size >= 0 {
		if err := os.Truncate(path, attr.Size); err != nil {
			return errors.Errorf("set file %s attr size %d failed: %w", path, attr.Size, err)
		}
	}

	if attr.Mode >= 0 {
		if err := os.Chmod(path, os.FileMode(attr.Mode)); err != nil {
			return errors.Errorf("set file %s attr mode %d failed: %w", path, attr.Mode, err)
		}
	}

	// not support change atime, mtime, ctime
	return nil
}
