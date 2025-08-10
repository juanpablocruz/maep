package transport

import (
	"testing"
	"time"
)

func TestTCPEndpoint_PrunesIdleConnections(t *testing.T) {
	ep, err := ListenTCP("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ep.Close()

	// Dial a connection by sending a frame to self (loopback)
	if err := ep.Send(ep.Addr(), []byte{0x1}); err != nil {
		t.Fatalf("send: %v", err)
	}
	// Allow accept to install conn
	time.Sleep(50 * time.Millisecond)

	// Force prune with very small idle threshold
	go ep.pruneLoop(50 * time.Millisecond)
	// Wait longer than idle
	time.Sleep(200 * time.Millisecond)

	ep.mu.Lock()
	nconns := len(ep.conns)
	ep.mu.Unlock()
	if nconns != 0 {
		t.Fatalf("expected connections to be pruned, still have %d", nconns)
	}
}