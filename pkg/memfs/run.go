package memfs

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/Sherlock-Holo/tfs/internal/memfs"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func Run(mountPoint string, debug bool) error {
	oneSecond := time.Second

	options := new(fs.Options)
	options.Debug = debug
	options.FsName = "empty"
	options.Name = "memfs"
	options.EntryTimeout = &oneSecond
	options.AttrTimeout = &oneSecond
	options.DisableXAttrs = true
	options.MaxReadAhead = 128 * 1024

	// allow mount point is not empty
	options.Options = append(options.Options, "nonempty")

	root := memfs.NewRoot(&fuse.Owner{
		Uid: uint32(os.Getuid()),
		Gid: uint32(os.Getgid()),
	})

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)

	server, err := fs.Mount(mountPoint, root, options)
	if err != nil {
		return fmt.Errorf("run memfs failed: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		server.Wait()
		cancel()
	}()

	select {
	case <-signalCh:
		if err := server.Unmount(); err != nil {
			return fmt.Errorf("unmount failed: %w", err)
		}

	case <-ctx.Done():
	}

	return nil
}
