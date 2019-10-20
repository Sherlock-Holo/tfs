package fs

import (
	"context"
	"io"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
	errors "golang.org/x/xerrors"
)

type File struct {
	*os.File

	deleteCtx   context.Context
	deleteFunc  context.CancelFunc
	timeoutCtx  context.Context
	timeoutFunc context.CancelFunc
	timer       *time.Timer
	allocateCh  chan AllocateRequest
	readCh      chan ReadRequest
	writeCh     chan WriteRequest
	timeoutCh   chan<- fileTimeoutCloseRequest
}

func newFile(f *os.File, timeoutCh chan<- fileTimeoutCloseRequest) *File {
	deleteCtx, deleteFunc := context.WithCancel(context.Background())
	timeoutCtx, timeoutFunc := context.WithCancel(context.Background())

	file := &File{
		File:        f,
		deleteCtx:   deleteCtx,
		deleteFunc:  deleteFunc,
		timeoutCtx:  timeoutCtx,
		timeoutFunc: timeoutFunc,
		timer:       time.NewTimer(fileTimeout),
		allocateCh:  make(chan AllocateRequest, fileAllocateChanSize),
		readCh:      make(chan ReadRequest, fileReadChanSize),
		writeCh:     make(chan WriteRequest, fileWriteChanSize),
		timeoutCh:   timeoutCh,
	}

	go file.handleLoop()

	return file
}

func (file *File) handleLoop() {
	// when a read/writeReq is blocking, and file timeout or deleted, file won't
	// handle read/writeReq, but will notice by delete/timeoutCtx so file won't
	// block FS.
	defer func() {
		file.timer.Stop()
		_ = file.Close()
	}()

	for {
		select {
		case <-file.deleteCtx.Done():
			// file deleted
			return

		case <-file.timer.C:
			// timeout, close file to reduce resource
			file.timeoutFunc()
			file.timeoutCh <- fileTimeoutCloseRequest{name: file.Name()}

			return

		case allocateReq := <-file.allocateCh:
			file.resetTimer(fileTimeout)

			ctx := allocateReq.Ctx
			respCh := allocateReq.RespCh

			size := int64(allocateReq.Offset + allocateReq.Size)
			if err := file.Truncate(size); err != nil {
				err = errors.Errorf("truncate %s size %d failed: %w", file.Name(), size, err)
				select {
				case <-ctx.Done():
					log.Errorf("%+v", err)

				case respCh <- err:
				}
				continue
			}
			select {
			case <-ctx.Done():
				log.Warnf("%+v", errors.Errorf("send truncate file %s response cancel or timeout: %w", file.Name(), ctx.Err()))

			case respCh <- nil:
			}

		case readReq := <-file.readCh:
			file.resetTimer(fileTimeout)

			ctx := readReq.Ctx
			respCh := readReq.RespCh

			offset := readReq.Offset
			size := readReq.Size
			buf := make([]byte, size)
			n, err := file.ReadAt(buf, int64(offset))

			var resp ReadResponse
			switch {
			default:
				resp = ReadResponse{
					Err: errors.Errorf("read file %s offset %d size %d failed: %w", file.Name(), offset, size, err),
				}

			case errors.Is(err, io.EOF), err == nil:
				resp = ReadResponse{
					Offset: offset,
					Data:   buf[:n],
				}
			}

			select {
			case <-ctx.Done():
				log.Warnf("%+v", errors.Errorf("send read file %s response cancel or timeout: %w", file.Name(), ctx.Err()))

			case respCh <- resp:
			}

		case writeReq := <-file.writeCh:
			file.resetTimer(fileTimeout)

			ctx := writeReq.Ctx
			respCh := writeReq.RespCh

			offset := writeReq.Offset
			written, err := file.WriteAt(writeReq.Data, int64(offset))
			var resp WriteResponse
			switch {
			default:
				err = errors.Errorf("write file %s at %d failed: %w", file.Name(), offset)
				resp = WriteResponse{Err: err}

			case err == nil:
				resp = WriteResponse{Written: written}
			}

			select {
			case <-ctx.Done():
				log.Errorf("%+v", errors.Errorf("send write file %s response cancel or timeout: %w", file.Name(), ctx.Err()))

			case respCh <- resp:
			}
		}
	}
}

func (file *File) resetTimer(duration time.Duration) {
	file.timer.Reset(duration)
	select {
	case <-file.timer.C:
	default:
	}
}

func (file *File) closeFile() {
	file.deleteFunc()
}
