package client

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/Sherlock-Holo/tfs/api/rpc"
	"github.com/Sherlock-Holo/tfs/internal/tfs/client"
	"github.com/Sherlock-Holo/tfs/pkg/tfs"
	"github.com/hanwen/go-fuse/v2/fs"
	log "github.com/sirupsen/logrus"
	errors "golang.org/x/xerrors"
	"google.golang.org/grpc"
)

func Run(cfg Config) error {
	fsOptions := initOptions(cfg)

	dialOptions := []grpc.DialOption{
		grpc.WithUnaryInterceptor(client.RetryInterceptor),
	}

	if cfg.InSecure {
		dialOptions = append(dialOptions, grpc.WithInsecure())
	}

	if err := os.Setenv("GRPC_GO_RETRY", "on"); err != nil {
		return errors.Errorf("set grpc retry environment failed: %w", err)
	}

	grpcConn, err := grpc.DialContext(context.TODO(), cfg.Address, dialOptions...)
	if err != nil {
		return errors.Errorf("dial tfs server %s failed: %w", cfg.Address, err)
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
		return errors.Errorf("mount tfs failed: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		server.Wait()
		cancel()
	}()

	select {
	case <-signalCh:
		var err error
		for i := 0; i < 3; i++ {
			if err = server.Unmount(); err != nil {
				log.Errorf("%+v", errors.Errorf("unmount tfs failed: %w", err))
			} else {
				break
			}

			time.Sleep(3 * time.Second)
		}
		if err != nil {
			return err
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
