package transport

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync"
)

type UDPEndpoint struct {
	c      *net.UDPConn
	addr   MemAddr
	in     chan []byte
	closed chan struct{}
	rmu    sync.Mutex
}

func ListenUDP(addr string) (*UDPEndpoint, error) {
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	c, err := net.ListenUDP("udp", ua)
	if err != nil {
		return nil, err
	}
	ep := &UDPEndpoint{
		c:      c,
		addr:   MemAddr(c.LocalAddr().String()),
		in:     make(chan []byte, 256),
		closed: make(chan struct{}),
	}
	go ep.readLoop()
	return ep, nil
}

func (e *UDPEndpoint) Addr() MemAddr { return e.addr }

func (e *UDPEndpoint) Close() {
	select {
	case <-e.closed:
		return
	default:
		close(e.closed)
		_ = e.c.Close()
	}
}

func (e *UDPEndpoint) Recv(ctx context.Context) ([]byte, bool) {
	select {
	case <-e.closed:
		return nil, false
	case <-ctx.Done():
		return nil, false
	case b := <-e.in:
		return b, true
	}
}

// Send writes one datagram (no extra framing).
func (e *UDPEndpoint) Send(to MemAddr, frame []byte) error {
	select {
	case <-e.closed:
		return errors.New("endpoint closed")
	default:
	}
	ra, err := net.ResolveUDPAddr("udp", string(to))
	if err != nil {
		return err
	}
	_, err = e.c.WriteToUDP(frame, ra)
	return err
}

func (e *UDPEndpoint) readLoop() {
	// Use a bufio.Reader-like buffer manually; UDP gives whole datagrams.
	buf := make([]byte, 64*1024)
	for {
		n, _, err := e.c.ReadFromUDP(buf)
		if err != nil {
			return
		}
		cp := make([]byte, n)
		copy(cp, buf[:n])
		select {
		case e.in <- cp:
		case <-e.closed:
			return
		}
	}
}

// satisfy (compile-time) that UDPEndpoint matches EndpointIF
var _ EndpointIF = (*UDPEndpoint)(nil)

// not used here, but keeps imports tidy
var _ = bufio.Reader{}
