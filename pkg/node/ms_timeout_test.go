package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/juanpablocruz/maep/pkg/protoport"
	"github.com/juanpablocruz/maep/pkg/transport"
)

type fakeTimeoutMessenger struct {
	sawSummaryReq chan error
}

func (f *fakeTimeoutMessenger) Recv(ctx context.Context) (transport.MemAddr, protoport.Message, bool) {
	// Never deliver anything; summaryLoop will still send
	select {
	case <-ctx.Done():
		return "", nil, false
	}
}

func (f *fakeTimeoutMessenger) Send(ctx context.Context, to transport.MemAddr, m protoport.Message) error {
	switch m.(type) {
	case protoport.SummaryReqMsg:
		// Block until ctx deadline then report the error back
		<-ctx.Done()
		select {
		case f.sawSummaryReq <- ctx.Err():
		default:
		}
		return ctx.Err()
	default:
		return nil
	}
}

func TestSummaryReq_UsesTimeoutContext(t *testing.T) {
	ms := &fakeTimeoutMessenger{sawSummaryReq: make(chan error, 1)}
	// Provide a heartbeat endpoint to avoid TCP fallback when EP is nil
	sw := transport.NewSwitch()
	hbEP, _ := sw.Listen("HB")
	defer hbEP.Close()

	n := NewWithOptions("T",
		WithMessenger(ms),
		WithPeer("P"),
		WithTickerEvery(50*time.Millisecond),
		WithHeartbeatEndpoint(hbEP),
	)
	// Disable descent so SummaryReq path is used
	n.DescentEnabled = false
	n.Start()
	defer n.Stop()

	select {
	case err := <-ms.sawSummaryReq:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected DeadlineExceeded, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("did not observe SummaryReq send with timeout")
	}
}