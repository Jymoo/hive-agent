package grpc

import (
	"context"
	"crypto/tls"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"time"
)

// Dial creates a TLS 1.3 gRPC client connection for future bidirectional stream transport.
func Dial(ctx context.Context, endpoint string, serverName string) (*grpc.ClientConn, error) {
	creds := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, ServerName: serverName})
	return grpc.DialContext(ctx, endpoint, grpc.WithTransportCredentials(creds), grpc.WithBlock(), grpc.WithTimeout(20*time.Second), grpc.WithDefaultCallOptions(grpc.UseCompressor("gzip")))
}
