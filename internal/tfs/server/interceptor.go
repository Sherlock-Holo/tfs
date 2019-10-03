package server

import (
	"context"

	"google.golang.org/grpc"
)

func CheckTimeoutInterceptor(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return handler(ctx, req)
}
