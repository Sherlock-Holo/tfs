package fs

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/Sherlock-Holo/tfs/api/rpc"
	"github.com/Sherlock-Holo/tfs/internal/tfs"
	log "github.com/sirupsen/logrus"
	errors "golang.org/x/xerrors"
)

type FS struct {
	Root               string
	files              map[string]*File
	allocateCh         chan AllocateRequest
	readCh             chan ReadRequest
	writeCh            chan WriteRequest
	attrCh             chan AttrRequest
	createCh           chan CreateRequest
	mkdirCh            chan MkdirRequest
	deleteFileCh       chan DeleteFileRequest
	rmdirCh            chan RmdirRequest
	readDirCh          chan ReadDirRequest
	lookupCh           chan LookupRequest
	renameCh           chan RenameRequest
	openFileCh         chan OpenFileRequest
	fileTimeoutCloseCh chan fileTimeoutCloseRequest
}

func (fs *FS) AllocateCh() chan<- AllocateRequest {
	return fs.allocateCh
}

func (fs *FS) ReadCh() chan<- ReadRequest {
	return fs.readCh
}

func (fs *FS) WriteCh() chan<- WriteRequest {
	return fs.writeCh
}

func (fs *FS) AttrCh() chan<- AttrRequest {
	return fs.attrCh
}

func (fs *FS) CreateCh() chan<- CreateRequest {
	return fs.createCh
}

func (fs *FS) MkdirCh() chan<- MkdirRequest {
	return fs.mkdirCh
}

func (fs *FS) DeleteFileCh() chan<- DeleteFileRequest {
	return fs.deleteFileCh
}

func (fs *FS) RmdirCh() chan<- RmdirRequest {
	return fs.rmdirCh
}

func (fs *FS) ReadDirCh() chan<- ReadDirRequest {
	return fs.readDirCh
}

func (fs *FS) LookupCh() chan<- LookupRequest {
	return fs.lookupCh
}

func (fs *FS) RenameCh() chan<- RenameRequest {
	return fs.renameCh
}

func (fs *FS) OpenFileCh() chan<- OpenFileRequest {
	return fs.openFileCh
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
func (fs *FS) handleLoop() {
	for {
		select {
		case attrReq := <-fs.attrCh:
			path := filepath.Join(fs.Root, attrReq.Name)
			ctx := attrReq.Ctx
			respCh := attrReq.RespCh

			var resp AttrResponse

			switch attrReq.Op {
			case AttrGet:
				info, err := os.Stat(path)
				switch err {
				default:
					resp.Err = errors.Errorf("get %s attr failed: %w", path, err)

				case nil:
					resp.Attr = tfs.CreateAttr(info)
				}

				select {
				case <-ctx.Done():
					log.Warnf("%+v", errors.Errorf("get file %s attr cancel or timeout: %w", path, err))

				case respCh <- resp:
				}

			case AttrSet:
				if err := setAttr(path, attrReq.Attr); err != nil {
					resp.Err = errors.Errorf("set file %s attr failed: %w", path, err)

					select {
					case <-ctx.Done():
						log.Warnf("%+v", errors.Errorf("set file %s attr cancel or timeout: %w", path, err))

					case respCh <- resp:
					}

					continue
				}

				info, err := os.Stat(path)
				if err != nil {
					resp.Err = errors.Errorf("get file %s attr failed: %w", path, err)

					select {
					case <-ctx.Done():
						log.Warnf("%+v", errors.Errorf("set file %s attr cancel or timeout: %w", path, err))

					case respCh <- resp:
					}

					continue
				}

				resp.Attr = tfs.CreateAttr(info)

				select {
				case <-ctx.Done():
					log.Warnf("%+v", errors.Errorf("set file %s attr cancel or timeout: %w", path, err))

				case respCh <- resp:
				}
			}

		case createReq := <-fs.createCh:
			path := filepath.Join(fs.Root, createReq.DirPath, createReq.Name)
			mode := createReq.Mode
			ctx := createReq.Ctx
			respCh := createReq.RespCh

			var resp CreateResponse

			if _, ok := fs.files[path]; ok {
				resp.Err = errors.Errorf("create file %s mode %d failed: %w", path, mode, os.ErrExist)
				select {
				case <-ctx.Done():
					log.Warnf("%+v", errors.Errorf("create file %s mode %d cancel or timeout: %w", path, mode, ctx.Err()))

				case respCh <- resp:
				}

				continue
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
					fs.files[path] = newFile(f, fs.fileTimeoutCloseCh)

					info, err := f.Stat()
					if err != nil {
						resp.Err = errors.Errorf("create file %s mode %d failed: %w", path, mode, err)
					} else {
						resp.Attr = tfs.CreateAttr(info)
					}
				}
			}

			respCh <- resp

		case mkdirReq := <-fs.mkdirCh:
			path := filepath.Join(fs.Root, mkdirReq.DirPath, mkdirReq.Name)
			ctx := mkdirReq.Ctx
			respCh := mkdirReq.RespCh

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
					resp.Attr = tfs.CreateAttr(info)
				}
			}

			select {
			case <-ctx.Done():
				log.Warnf("%+v", errors.Errorf("mkdir %s cancel or timeout: %w", ctx.Err()))

			case respCh <- resp:
			}

		case allocateReq := <-fs.allocateCh:
			path := filepath.Join(fs.Root, allocateReq.Name)
			ctx := allocateReq.Ctx
			respCh := allocateReq.RespCh

			file, ok := fs.files[path]
			if !ok {
				f, err := os.OpenFile(path, os.O_RDWR, 0)
				switch {
				default:
					err = errors.Errorf("allocate file %s failed: %w", path, err)

				case errors.Is(err, os.ErrNotExist):
					err = errors.Errorf("allocate file %s failed: %w", path, os.ErrNotExist)

				case err == nil:
					file = newFile(f, fs.fileTimeoutCloseCh)
				}

				if err != nil {
					select {
					case <-ctx.Done():
						log.Warnf("%+v", errors.Errorf("allocate file %s cancel or timeout: %w", path, ctx.Err()))

					case respCh <- err:
					}

					continue
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
					fs.allocateCh <- allocateReq
				}()

			case file.allocateCh <- allocateReq:
			}

		case readReq := <-fs.readCh:
			path := filepath.Join(fs.Root, readReq.Name)
			ctx := readReq.Ctx
			respCh := readReq.RespCh

			file, ok := fs.files[path]
			if !ok {
				f, err := os.OpenFile(path, os.O_RDWR, 0)
				switch {
				default:
					err = errors.Errorf("read file %s failed: %w", path, err)

				case errors.Is(err, os.ErrNotExist):
					err = errors.Errorf("read file %s failed: %w", path, os.ErrNotExist)

				case err == nil:
					file = newFile(f, fs.fileTimeoutCloseCh)
				}

				if err != nil {
					select {
					case <-ctx.Done():
						log.Warnf("%+v", errors.Errorf("read file %s cancel or timeout: %w", path, ctx.Err()))

					case respCh <- ReadResponse{Err: err}:
					}

					continue
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
					fs.readCh <- readReq
				}()

			case file.readCh <- readReq:
			}

		case writeReq := <-fs.writeCh:
			path := filepath.Join(fs.Root, writeReq.Name)
			ctx := writeReq.Ctx
			respCh := writeReq.RespCh

			file, ok := fs.files[path]
			if !ok {
				f, err := os.OpenFile(path, os.O_RDWR, 0)
				switch {
				default:
					err = errors.Errorf("write file %s failed: %w", path, err)

				case errors.Is(err, os.ErrNotExist):
					err = errors.Errorf("write file %s failed: %w", path, os.ErrNotExist)

				case err == nil:
					file = newFile(f, fs.fileTimeoutCloseCh)
				}

				if err != nil {
					select {
					case <-ctx.Done():
						log.Warnf("%+v", errors.Errorf("write file %s cancel or timeout: %w", path, ctx.Err()))

					case respCh <- WriteResponse{Err: err}:
					}

					continue
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
					fs.writeCh <- writeReq
				}()

			case file.writeCh <- writeReq:
			}

		case deleteFileReq := <-fs.deleteFileCh:
			path := filepath.Join(fs.Root, deleteFileReq.DirPath, deleteFileReq.Name)
			ctx := deleteFileReq.Ctx
			respCh := deleteFileReq.RespCh

			if file, ok := fs.files[path]; ok {
				file.closeFile()
				delete(fs.files, path)
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

		case rmdirReq := <-fs.rmdirCh:
			path := filepath.Join(fs.Root, rmdirReq.DirPath, rmdirReq.Name)
			ctx := rmdirReq.Ctx
			respCh := rmdirReq.RespCh

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

		case readDirReq := <-fs.readDirCh:
			path := filepath.Join(fs.Root, readDirReq.Name)
			ctx := readDirReq.Ctx
			respCh := readDirReq.RespCh

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

		case lookupReq := <-fs.lookupCh:
			path := filepath.Join(lookupReq.DirPath, lookupReq.Name)
			ctx := lookupReq.Ctx
			respCh := lookupReq.RespCh

			var resp LookupResponse

			info, err := os.Stat(path)
			switch {
			default:
				resp.Err = errors.Errorf("lookup %s failed: %w", path, err)

			case errors.Is(err, os.ErrNotExist):
				resp.Err = errors.Errorf("lookup %s failed: %w", path, os.ErrNotExist)

			case err == nil:
				resp.Attr = tfs.CreateAttr(info)
			}

			select {
			case <-ctx.Done():
				log.Warnf("%+v", errors.Errorf("lookup %s cancel or timeout: %w", path, ctx.Err()))

			case respCh <- resp:
			}

		case renameReq := <-fs.renameCh:
			oldPath := filepath.Join(fs.Root, renameReq.OldDirPath, renameReq.OldName)
			newPath := filepath.Join(fs.Root, renameReq.NewDirPath, renameReq.NewName)
			ctx := renameReq.Ctx
			respCh := renameReq.RespCh

			if _, ok := fs.files[newPath]; ok {
				select {
				case <-ctx.Done():
					log.Warnf("%+v", errors.Errorf("rename old file %s to new file %s cancel or timeout: %w", oldPath, newPath, ctx.Err()))

				case respCh <- errors.Errorf("rename old file %s to new file %s failed: %w", oldPath, newPath, os.ErrExist):
				}

				continue
			}

			if file, ok := fs.files[oldPath]; ok {
				file.closeFile()
				delete(fs.files, oldPath)
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

		case openFileReq := <-fs.openFileCh:
			path := filepath.Join(fs.Root, openFileReq.Name)
			ctx := openFileReq.Ctx
			respCh := openFileReq.RespCh

			var (
				resp   OpenFileResponse
				file   *File
				opened bool
			)

			for {
				if file, opened = fs.files[path]; opened {
					break
				}

				f, err := os.OpenFile(path, os.O_RDWR, 0)
				switch {
				default:
					resp.Err = errors.Errorf("open file %s failed: %w", path, err)

				case errors.Is(err, os.ErrNotExist):
					resp.Err = errors.Errorf("open file %s failed: %w", path, os.ErrNotExist)

				case err == nil:
					fs.files[path] = newFile(f, fs.fileTimeoutCloseCh)
				}
			}

			info, err := file.Stat()
			switch err {
			default:
				resp.Err = errors.Errorf("open file %s failed: %w", path, err)

			case nil:
				resp.Attr = tfs.CreateAttr(info)
			}

			select {
			case <-ctx.Done():
				log.Warnf("open file %s cancel or timeout: %w", path, ctx.Err())

			case respCh <- resp:
			}

		case fileTimeoutCloseReq := <-fs.fileTimeoutCloseCh:
			// file timeout, close to reduce resource
			delete(fs.files, fileTimeoutCloseReq.name)
		}
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
