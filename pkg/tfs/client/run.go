package client

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/Sherlock-Holo/tfs/api/rpc"
	"github.com/Sherlock-Holo/tfs/internal/tfs/client"
	"github.com/Sherlock-Holo/tfs/pkg/tfs"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

func Run(cfg Config) error {
	fsOptions := initOptions(cfg)

	var dialOptions []grpc.DialOption
	if cfg.InSecure {
		dialOptions = append(dialOptions, grpc.WithInsecure())
	}

	grpcConn, err := grpc.DialContext(context.TODO(), cfg.Address, dialOptions...)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("dial tfs server %s failed", cfg.Address))
	}
	defer func() {
		_ = grpcConn.Close()
	}()

	grpcClient := rpc.NewTfsClient(grpcConn)

	root := client.NewRoot(&client.Client{TfsClient: grpcClient})

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)

	server, err := fs.Mount(cfg.MountPoint, root, fsOptions)
	if err != nil {
		return errors.Wrap(err, "mount tfs failed")
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		server.Wait()
		cancel()
	}()

	select {
	case <-signalCh:
		if err := server.Unmount(); err != nil {
			return errors.Wrap(err, "unmount tfs failed")
		}

	case <-ctx.Done():
	}

	return nil
}

func initOptions(cfg Config) *fs.Options {
	oneSecond := time.Second

	options := new(fs.Options)
	options.Debug = cfg.Debug
	options.FsName = cfg.TargetName
	options.Name = tfs.FSName
	options.EntryTimeout = &oneSecond
	options.AttrTimeout = &oneSecond
	options.DisableXAttrs = true
	options.MaxReadAhead = 128 * 1024

	// allow mount point is not empty
	options.Options = append(options.Options, "nonempty")

	return options
}
