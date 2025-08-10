package node_test

import (
	"testing"
	"time"

	"github.com/juanpablocruz/maep/pkg/hlc"
	"github.com/juanpablocruz/maep/pkg/model"
	node "github.com/juanpablocruz/maep/pkg/node"
	testutil "github.com/juanpablocruz/maep/pkg/node/testutil"
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
	n := node.NewWithOptions("NB",
		node.WithEndpoint(b),
		node.WithPeer("A"),
		node.WithTickerEvery(100*time.Millisecond),
		node.WithLog(oplog.New()),
		node.WithClock(hlc.New()),
	)
	ec := testutil.NewEventCollector(256)
	ec.Attach(n)
	n.Start()
	defer n.Stop()
	defer ec.Detach(n)

	// Send an in-order chunk (seq 0)
	op := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "x", HLCTicks: n.Clock.Now()}
	op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
	ch0 := syncproto.DeltaChunk{SID: 0, Seq: 0, Last: false, Entries: []syncproto.DeltaEntry{{Key: "x", Ops: []model.Op{op}}}}
	if err := a.Send("B", wire.Encode(wire.MT_SYNC_DELTA_CHUNK, syncproto.EncodeDeltaChunk(ch0))); err != nil {
		t.Fatal(err)
	}

	// Wait a bit for ACK
	time.Sleep(20 * time.Millisecond)

	// Now send a duplicate of seq 0 again; NB should only ACK last-in-order, not send a NACK(duplicate)
	if err := a.Send("B", wire.Encode(wire.MT_SYNC_DELTA_CHUNK, syncproto.EncodeDeltaChunk(ch0))); err != nil {
		t.Fatal(err)
	}

	ok := ec.WaitFor(200*time.Millisecond, func(evts []node.Event) bool {
		sawAckDuplicate := false
		sawNackDuplicate := false
		for _, e := range evts {
			if e.Type != node.EventAck {
				continue
			}
			if dir, _ := e.Fields["dir"].(string); dir != "send" {
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
		return sawAckDuplicate && !sawNackDuplicate
	})
	if !ok {
		t.Fatalf("expected ACK(reason=duplicate) and no NACK on duplicate chunk")
	}
}

// Test out-of-window future chunk (seq > expect) triggers NACK(out_of_window).
func TestFutureChunkNackOutOfWindow(t *testing.T) {
	sw := transport.NewSwitch()
	a, _ := sw.Listen("A")
	b, _ := sw.Listen("B")
	defer a.Close()
	defer b.Close()

	n := node.NewWithOptions("NB",
		node.WithEndpoint(b),
		node.WithPeer("A"),
		node.WithTickerEvery(100*time.Millisecond),
		node.WithLog(oplog.New()),
		node.WithClock(hlc.New()),
	)
	ec := testutil.NewEventCollector(256)
	ec.Attach(n)
	n.Start()
	defer n.Stop()
	defer ec.Detach(n)

	// Send seq=1 while expect=0
	op := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "y", HLCTicks: n.Clock.Now()}
	op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
	ch1 := syncproto.DeltaChunk{SID: 0, Seq: 1, Last: false, Entries: []syncproto.DeltaEntry{{Key: "y", Ops: []model.Op{op}}}}
	if err := a.Send("B", wire.Encode(wire.MT_SYNC_DELTA_CHUNK, syncproto.EncodeDeltaChunk(ch1))); err != nil {
		t.Fatal(err)
	}

	ok := ec.WaitFor(200*time.Millisecond, func(evts []node.Event) bool {
		for _, e := range evts {
			if e.Type != node.EventAck {
				continue
			}
			if _, isNack := e.Fields["nack"].(bool); isNack {
				if r, _ := e.Fields["reason"].(string); r == "out_of_window" {
					return true
				}
			}
		}
		return false
	})
	if !ok {
		t.Fatalf("expected NACK(reason=out_of_window) for future chunk")
	}
}

// Spec compliance: In-order chunk applies ops, merges HLC, and ACKs
func TestApplyDeltaChunk_InOrder_AckAndHLCMerge(t *testing.T) {
	sw := transport.NewSwitch()
	a, _ := sw.Listen("A")
	b, _ := sw.Listen("B")
	defer a.Close()
	defer b.Close()

	n := node.NewWithOptions("NB",
		node.WithEndpoint(b),
		node.WithPeer("A"),
		node.WithTickerEvery(100*time.Millisecond),
		node.WithLog(oplog.New()),
		node.WithClock(hlc.New()),
	)
	ec := testutil.NewEventCollector(256)
	ec.Attach(n)
	n.Start()
	defer n.Stop()
	defer ec.Detach(n)

	// Build a chunk with HLC higher than node clock
	var actor model.ActorID
	actor[0] = 0x01
	hlcTick := n.Clock.Now() + 1000
	op := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "k", Value: []byte("v"), HLCTicks: hlcTick, Actor: actor}
	op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
	ch := syncproto.DeltaChunk{SID: 0, Seq: 0, Last: true, Entries: []syncproto.DeltaEntry{{Key: "k", Ops: []model.Op{op}}}}

	if err := a.Send("B", wire.Encode(wire.MT_SYNC_DELTA_CHUNK, syncproto.EncodeDeltaChunk(ch))); err != nil {
		t.Fatal(err)
	}

	ok := ec.WaitFor(200*time.Millisecond, func(evts []node.Event) bool {
		for _, e := range evts {
			if e.Type == node.EventAck {
				if dir, _ := e.Fields["dir"].(string); dir == "send" {
					if seq, _ := e.Fields["seq"].(uint32); seq == 0 {
						return true
					}
				}
			}
		}
		return false
	})
	if !ok {
		t.Fatalf("expected ACK seq 0 to be sent")
	}
	snap := n.Log.Snapshot()
	if ops := snap["k"]; len(ops) != 1 || string(ops[0].Value) != "v" {
		t.Fatalf("op not applied: %+v", ops)
	}
	// HLC should have merged to at least hlcTick
	if n.Clock.Now() < hlcTick {
		t.Fatalf("clock did not merge to max tick: now=%d want>=%d", n.Clock.Now(), hlcTick)
	}
}

// Spec compliance: Duplicate chunk replay should not re-append operations (idempotence)
func TestIdempotentChunkReplay_NoDuplication(t *testing.T) {
	sw := transport.NewSwitch()
	a, _ := sw.Listen("A")
	b, _ := sw.Listen("B")
	defer a.Close()
	defer b.Close()

	n := node.NewWithOptions("NB",
		node.WithEndpoint(b),
		node.WithPeer("A"),
		node.WithTickerEvery(100*time.Millisecond),
		node.WithLog(oplog.New()),
		node.WithClock(hlc.New()),
	)
	ec := testutil.NewEventCollector(256)
	ec.Attach(n)
	n.Start()
	defer n.Stop()
	defer ec.Detach(n)

	var actor model.ActorID
	actor[0] = 0x02
	op := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "k", Value: []byte("v"), HLCTicks: n.Clock.Now(), Actor: actor}
	op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
	ch := syncproto.DeltaChunk{SID: 0, Seq: 0, Last: false, Entries: []syncproto.DeltaEntry{{Key: "k", Ops: []model.Op{op}}}}
	enc := syncproto.EncodeDeltaChunk(ch)
	if err := a.Send("B", wire.Encode(wire.MT_SYNC_DELTA_CHUNK, enc)); err != nil {
		t.Fatal(err)
	}
	if err := a.Send("B", wire.Encode(wire.MT_SYNC_DELTA_CHUNK, enc)); err != nil {
		t.Fatal(err)
	}

	time.Sleep(30 * time.Millisecond)
	if ops := n.Log.Snapshot()["k"]; len(ops) != 1 {
		t.Fatalf("expected single op after duplicate replay, got %d", len(ops))
	}
}
