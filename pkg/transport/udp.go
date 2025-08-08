package transport

import (
	"context"
	"errors"
	"net"
	"sync"
)

type udpEnv struct {
	from MemAddr
	data []byte
}

type UDPEndpoint struct {
	c      *net.UDPConn
	addr   MemAddr
	in     chan udpEnv
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
		in:     make(chan udpEnv, 256),
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
	_, b, ok := e.RecvFrom(ctx)
	return b, ok
}
func (e *UDPEndpoint) RecvFrom(ctx context.Context) (MemAddr, []byte, bool) {
	select {
	case <-e.closed:
		return "", nil, false
	case <-ctx.Done():
		return "", nil, false
	case env := <-e.in:
		return env.from, env.data, true
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
		n, raddr, err := e.c.ReadFromUDP(buf)
		if err != nil {
			return
		}
		b := make([]byte, n)
		copy(b, buf[:n])
		select {
		case e.in <- udpEnv{from: MemAddr(raddr.String()), data: b}:
		case <-e.closed:
			return
		}
	}
}
