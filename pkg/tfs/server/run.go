package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/Sherlock-Holo/tfs/api/rpc"
	"github.com/Sherlock-Holo/tfs/internal/tfs/server"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

func Run(cfg Config) error {
	var options []grpc.ServerOption

	// add context timeout check
	options = append(options, grpc.UnaryInterceptor(server.CheckTimeoutInterceptor))

	grpcServer := grpc.NewServer(options...)

	rpc.RegisterTfsServer(grpcServer, &server.Server{Root: cfg.Root})

	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("listen %s failed", cfg.Address))
	}

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
		return errors.Wrap(err, "server closed")
	}

	<-shutdown.Done()

	if timeoutFlag {
		return errors.New("server shutdown timeout")
	}
	return nil
}
