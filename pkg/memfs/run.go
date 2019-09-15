package memfs

import (
	"fmt"
	"os"
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
	// allow mount point is not empty
	options.Options = append(options.Options, "nonempty")
	options.DisableXAttrs = true

	root := memfs.NewRoot(&fuse.Owner{
		Uid: uint32(os.Getuid()),
		Gid: uint32(os.Getgid()),
	})

	rawFs := fs.NewNodeFS(root, options)

	server, err := fuse.NewServer(rawFs, mountPoint, &options.MountOptions)
	if err != nil {
		return fmt.Errorf("new fuse server failed: %w", err)
	}

	go server.Serve()

	if err := server.WaitMount(); err != nil {
		return fmt.Errorf("server wait mount failed: %w", err)
	}

	server.Wait()

	return nil
}
