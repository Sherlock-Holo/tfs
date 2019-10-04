package server

import (
	"context"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/Sherlock-Holo/tfs/api/rpc"
	"github.com/Sherlock-Holo/tfs/internal/tfs/server"
	log "github.com/sirupsen/logrus"
	errors "golang.org/x/xerrors"
	"google.golang.org/grpc"
)

func Run(cfg Config) error {

	var options []grpc.ServerOption

	// add context timeout check
	options = append(options, grpc.UnaryInterceptor(server.CheckTimeoutInterceptor))

	grpcServer := grpc.NewServer(options...)

	if err := chroot(cfg.Root); err != nil {
		return errors.Errorf("chroot to %s failed: %w", cfg.Root, err)
	}

	log.Debugln("chroot success")

	rpc.RegisterTfsServer(grpcServer, server.NewServer("/"))

	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return errors.Errorf("listen %s failed: %w", cfg.Address, err)
	}

	log.Debugf("listen %s success", cfg.Address)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)

	var timeoutFlag bool
	shutdown, shutdownCancel := context.WithCancel(context.Background())
	go func() {
		defer shutdownCancel()

		<-signalCh

		if cfg.ShutdownTimeout == 0 {
			cfg.ShutdownTimeout = 3 * time.Second
		}

		timeout, timeoutCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		go func() {
			grpcServer.GracefulStop()
			timeoutCancel()
		}()

		<-timeout.Done()
		// if timeout, stop immediately
		if timeout.Err() == context.DeadlineExceeded {
			grpcServer.Stop()
			timeoutFlag = true
		}
	}()

	if err := grpcServer.Serve(listener); err != nil {
		return errors.Errorf("server closed: %w", err)
	}

	<-shutdown.Done()

	if timeoutFlag {
		return errors.New("server shutdown timeout")
	}
	return nil
}
