package transport

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

type tcpEnv struct {
	from MemAddr
	data []byte
}

// TCPEndpoint implements EndpointIF over TCP.
// Address strings are just MemAddr (alias of string).
type TCPEndpoint struct {
	ln     net.Listener
	addr   MemAddr
	in     chan tcpEnv
	closed chan struct{}

	mu    sync.Mutex
	conns map[MemAddr]*tcpPeer // dialed peers by remote address
}

type tcpPeer struct {
	addr MemAddr
	c    net.Conn
	r    *bufio.Reader
	wmu  sync.Mutex

	mu          sync.Mutex
	lastActUnix int64
}

func ListenTCP(addr string) (*TCPEndpoint, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	ep := &TCPEndpoint{
		ln:     ln,
		addr:   MemAddr(ln.Addr().String()),
		in:     make(chan tcpEnv, 128),
		closed: make(chan struct{}),
		conns:  make(map[MemAddr]*tcpPeer),
	}
	go ep.acceptLoop()
	go ep.pruneLoop(2 * time.Minute)
	return ep, nil
}

func (e *TCPEndpoint) Addr() MemAddr { return e.addr }

func (e *TCPEndpoint) Close() {
	select {
	case <-e.closed:
		return
	default:
		close(e.closed)
		_ = e.ln.Close()
		e.mu.Lock()
		for _, p := range e.conns {
			_ = p.c.Close()
		}
		e.conns = map[MemAddr]*tcpPeer{}
		e.mu.Unlock()
	}
}

func (e *TCPEndpoint) Recv(ctx context.Context) ([]byte, bool) {
	_, b, ok := e.RecvFrom(ctx)
	return b, ok
}

func (e *TCPEndpoint) RecvFrom(ctx context.Context) (MemAddr, []byte, bool) {
	select {
	case <-e.closed:
		return "", nil, false
	case <-ctx.Done():
		return "", nil, false
	case b := <-e.in:
		return b.from, b.data, true
	}
}

func (e *TCPEndpoint) Send(to MemAddr, frame []byte) error {
	select {
	case <-e.closed:
		return errors.New("endpoint closed")
	default:
	}

	p := e.getOrDial(to)
	if p == nil {
		return errors.New("dial failed")
	}
	p.wmu.Lock()
	// prevent indefinite blocking on slow/broken peers
	_ = p.c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	err := writeFrame(p.c, frame)
	// clear deadline for future ops
	_ = p.c.SetWriteDeadline(time.Time{})
	p.wmu.Unlock()
	if err != nil {
		// drop broken conn from cache so next Send() will re-dial
		e.mu.Lock()
		if cur, ok := e.conns[to]; ok && cur == p {
			_ = cur.c.Close()
			delete(e.conns, to)
		}
		e.mu.Unlock()
		return err
	}
	p.mu.Lock()
	p.lastActUnix = time.Now().Unix()
	p.mu.Unlock()
	return nil
}

func (e *TCPEndpoint) getOrDial(to MemAddr) *tcpPeer {
	e.mu.Lock()
	if p, ok := e.conns[to]; ok {
		e.mu.Unlock()
		return p
	}
	e.mu.Unlock()

	// dial without holding lock
	d, err := net.DialTimeout("tcp", string(to), 2*time.Second)
	if err != nil {
		return nil
	}
	p := &tcpPeer{addr: to, c: d, r: bufio.NewReader(d)}
	p.lastActUnix = time.Now().Unix()

	// reader goroutine (for replies/unsolicited frames)
	go func() {
		defer d.Close()
		for {
			b, err := readFrame(p.r)
			if err != nil {
				return
			}
			p.mu.Lock()
			p.lastActUnix = time.Now().Unix()
			p.mu.Unlock()
			select {
			case e.in <- tcpEnv{from: to, data: b}:
			case <-e.closed:
				return
			}
		}
	}()

	// store in cache
	e.mu.Lock()
	// if someone raced and stored a conn, close this one and use the cached
	if existing, ok := e.conns[to]; ok {
		e.mu.Unlock()
		_ = d.Close()
		return existing
	}
	e.conns[to] = p
	e.mu.Unlock()
	return p
}

func (e *TCPEndpoint) acceptLoop() {
	for {
		c, err := e.ln.Accept()
		if err != nil {
			select {
			case <-e.closed:
				return
			default:
				// brief sleep to avoid tight loop on transient errors
				time.Sleep(50 * time.Millisecond)
				continue
			}
		}
		go e.handleConn(c)
	}
}

func (e *TCPEndpoint) handleConn(c net.Conn) {
	peer := MemAddr(c.RemoteAddr().String())
	p := &tcpPeer{addr: peer, c: c, r: bufio.NewReader(c)}
	p.lastActUnix = time.Now().Unix()

	// make this inbound conn replyable
	e.mu.Lock()
	e.conns[peer] = p
	e.mu.Unlock()

	defer func() {
		// remove only if still the same pointer
		e.mu.Lock()
		if cur, ok := e.conns[peer]; ok && cur == p {
			delete(e.conns, peer)
		}
		e.mu.Unlock()
		_ = c.Close()
	}()

	for {
		b, err := readFrame(p.r)
		if err != nil {
			return
		}
		p.mu.Lock()
		p.lastActUnix = time.Now().Unix()
		p.mu.Unlock()
		select {
		case e.in <- tcpEnv{from: peer, data: b}:
		case <-e.closed:
			return
		}
	}
}

func (e *TCPEndpoint) pruneLoop(maxIdle time.Duration) {
	t := time.NewTicker(maxIdle / 2)
	defer t.Stop()
	for {
		select {
		case <-e.closed:
			return
		case <-t.C:
			now := time.Now().Unix()
			cut := now - int64(maxIdle.Seconds())
			e.mu.Lock()
			for addr, p := range e.conns {
				p.mu.Lock()
				last := p.lastActUnix
				p.mu.Unlock()
				if last != 0 && last < cut {
					_ = p.c.Close()
					delete(e.conns, addr)
				}
			}
			e.mu.Unlock()
		}
	}
}
