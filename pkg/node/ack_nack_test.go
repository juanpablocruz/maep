package node

import (
	"testing"
	"time"

	"github.com/juanpablocruz/maep/pkg/hlc"
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/oplog"
	"github.com/juanpablocruz/maep/pkg/syncproto"
	"github.com/juanpablocruz/maep/pkg/transport"
	"github.com/juanpablocruz/maep/pkg/wire"
)

// Test that duplicate chunk (seq < expect) elicits only an ACK(last-in-order) and no NACK.
func TestDuplicateChunkAckNoNack(t *testing.T) {
	sw := transport.NewSwitch()
	a, _ := sw.Listen("A")
	b, _ := sw.Listen("B")
	defer a.Close()
	defer b.Close()

	// Node on B receiving from A
	ev := make(chan Event, 64)
	n := New("NB", b, "A", 100*time.Millisecond)
	n.Log = oplog.New()
	n.Clock = hlc.New()
	n.AttachEvents(ev)
	n.Start()
	defer n.Stop()

	// Send an in-order chunk (seq 0)
	op := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "x", HLCTicks: n.Clock.Now()}
	op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor)
	ch0 := syncproto.DeltaChunk{Seq: 0, Last: false, Entries: []syncproto.DeltaEntry{{Key: "x", Ops: []model.Op{op}}}}
	if err := a.Send("B", wire.Encode(wire.MT_SYNC_DELTA_CHUNK, syncproto.EncodeDeltaChunk(ch0))); err != nil {
		t.Fatal(err)
	}

	// Wait a bit for ACK
	time.Sleep(20 * time.Millisecond)

	// Now send a duplicate of seq 0 again; NB should only ACK last-in-order, not send a NACK(duplicate)
	if err := a.Send("B", wire.Encode(wire.MT_SYNC_DELTA_CHUNK, syncproto.EncodeDeltaChunk(ch0))); err != nil {
		t.Fatal(err)
	}

	// Collect events briefly
	time.Sleep(30 * time.Millisecond)
	close(ev)
	sawAckDuplicate := false
	sawNackDuplicate := false
	for e := range ev {
		if e.Type != EventAck {
			continue
		}
		dir, _ := e.Fields["dir"].(string)
		if dir != "send" {
			continue
		}
		if r, _ := e.Fields["reason"].(string); r == "duplicate" {
			if _, isNack := e.Fields["nack"].(bool); isNack {
				sawNackDuplicate = true
			} else {
				sawAckDuplicate = true
			}
		}
	}
	if !sawAckDuplicate {
		t.Fatalf("expected ACK(reason=duplicate) on duplicate chunk")
	}
	if sawNackDuplicate {
		t.Fatalf("did not expect NACK(reason=duplicate) on duplicate chunk")
	}
}

// Test out-of-window future chunk (seq > expect) triggers NACK(out_of_window).
func TestFutureChunkNackOutOfWindow(t *testing.T) {
	sw := transport.NewSwitch()
	a, _ := sw.Listen("A")
	b, _ := sw.Listen("B")
	defer a.Close()
	defer b.Close()

	ev := make(chan Event, 64)
	n := New("NB", b, "A", 100*time.Millisecond)
	n.Log = oplog.New()
	n.Clock = hlc.New()
	n.AttachEvents(ev)
	n.Start()
	defer n.Stop()

	// Send seq=1 while expect=0
	op := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "y", HLCTicks: n.Clock.Now()}
	op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor)
	ch1 := syncproto.DeltaChunk{Seq: 1, Last: false, Entries: []syncproto.DeltaEntry{{Key: "y", Ops: []model.Op{op}}}}
	if err := a.Send("B", wire.Encode(wire.MT_SYNC_DELTA_CHUNK, syncproto.EncodeDeltaChunk(ch1))); err != nil {
		t.Fatal(err)
	}

	time.Sleep(30 * time.Millisecond)
	close(ev)
	sawNack := false
	for e := range ev {
		if e.Type != EventAck {
			continue
		}
		if _, isNack := e.Fields["nack"].(bool); isNack {
			if r, _ := e.Fields["reason"].(string); r == "out_of_window" {
				sawNack = true
			}
		}
	}
	if !sawNack {
		t.Fatalf("expected NACK(reason=out_of_window) for future chunk")
	}
}
