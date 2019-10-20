package client

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func RetryInterceptor(ctx context.Context, method string, req, resp interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) (err error) {
	for i := 0; i < 3; i++ {
		if err = invoker(ctx, method, req, resp, cc, opts...); err != nil {
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.Unavailable {
					log.Warnf("method %s, grpc server is unavailable, retrying...", method)

					time.Sleep(time.Duration(i) * time.Second)

					continue
				}
			}
		}
		break
	}

	return
}
