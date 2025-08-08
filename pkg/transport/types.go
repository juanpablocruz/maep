package transport

import "context"

// EndpointIF is the minimal surface Node needs.
// Both Mem Endpoint (mem.go) and the TCP endpoint will satisfy this.
type EndpointIF interface {
	Recv(ctx context.Context) ([]byte, bool)
	Send(to MemAddr, frame []byte) error
	Close()
}
