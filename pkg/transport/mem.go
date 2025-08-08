package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type MemAddr string

type memEnvelope struct {
	from MemAddr
	data []byte
}

// Switch delivers frames between listened addresses.
type Switch struct {
	mu    sync.RWMutex
	inbox map[MemAddr]chan memEnvelope
}

func NewSwitch() *Switch {
	return &Switch{inbox: make(map[MemAddr]chan memEnvelope)}
}

// handle a node uses to send/recv frames.
type Endpoint struct {
	sw     *Switch
	addr   MemAddr
	in     chan memEnvelope
	closed chan struct{}
}

func (s *Switch) Listen(addr MemAddr) (*Endpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.inbox[addr]; exists {
		return nil, fmt.Errorf("address already in use: %s", addr)
	}
	ch := make(chan memEnvelope, 128)
	s.inbox[addr] = ch
	return &Endpoint{
		sw: s, addr: addr, in: ch, closed: make(chan struct{}),
	}, nil
}

func (s *Switch) Unlisten(addr MemAddr) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.inbox[addr]; !ok {
		return false
	}
	delete(s.inbox, addr)
	return true
}

func (e *Endpoint) Addr() MemAddr { return e.addr }

func (e *Endpoint) Close() {
	select {
	case <-e.closed:
		return
	default:
		close(e.closed)
		e.sw.mu.Lock()
		delete(e.sw.inbox, e.addr)
		e.sw.mu.Unlock()
	}
}

func (e *Endpoint) Recv(ctx context.Context) ([]byte, bool) {
	_, b, ok := e.RecvFrom(ctx)
	return b, ok
}

// Recv blocks until a frame arrives or ctx/endpoint is closed.
func (e *Endpoint) RecvFrom(ctx context.Context) (MemAddr, []byte, bool) {
	select {
	case <-e.closed:
		return "", nil, false
	case <-ctx.Done():
		return "", nil, false
	case env := <-e.in:
		return env.from, env.data, true
	}
}

// Send delivers a frame to the destination address.
func (e *Endpoint) Send(to MemAddr, frame []byte) error {
	e.sw.mu.RLock()
	dst, ok := e.sw.inbox[to]
	e.sw.mu.RUnlock()
	if !ok {
		return errors.New("unknown destination")
	}
	select {
	case <-e.closed:
		return errors.New("endpoint closed")
	default:
	}
	select {
	case dst <- memEnvelope{from: e.addr, data: frame}:
		return nil
	default:
		// backpressure / drop policy; for now blockless error
		return errors.New("destination inbox full")
	}
}
