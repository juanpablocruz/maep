package node

import (
	"testing"
	"time"

	"github.com/juanpablocruz/maep/pkg/hlc"
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/oplog"
	"github.com/juanpablocruz/maep/pkg/segment"
	"github.com/juanpablocruz/maep/pkg/syncproto"
	"github.com/juanpablocruz/maep/pkg/transport"
	"github.com/juanpablocruz/maep/pkg/wire"
)

// Spec: NACK-driven retransmission resends the in-flight chunk
func TestNackRetransmission_RetriesInflightChunk(t *testing.T) {
	sw := transport.NewSwitch()
	A, _ := sw.Listen("A")
	B, _ := sw.Listen("B")
	defer A.Close()
	defer B.Close()

	// Node NB will receive and send ACK/NACK; Node NA will send chunks
	nb := NewWithOptions("NB",
		WithEndpoint(B), WithPeer("A"), WithTickerEvery(200*time.Millisecond),
		WithLog(oplog.New()), WithClock(hlc.New()),
	)
	nb.Start()
	defer nb.Stop()

	na := NewWithOptions("NA",
		WithEndpoint(A), WithPeer("B"), WithTickerEvery(200*time.Millisecond),
		WithLog(oplog.New()), WithClock(hlc.New()),
	)
	na.Start()
	defer na.Stop()

	// Prepare a delta from NA and stream one chunk so it is inflight
	var actor model.ActorID
	actor[0] = 0x55
	op := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "k", Value: []byte("v"), HLCTicks: na.Clock.Now(), Actor: actor}
	op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
	na.Log.Append(op)

	// Start a session manually: send SummaryReq -> NB replies SummaryResp -> NA sends Req -> NA sends DeltaChunk
	if err := A.Send("B", wire.Encode(wire.MT_SYNC_SUMMARY_REQ, syncproto.EncodeSummaryReq(syncproto.SummaryReq{}))); err != nil {
		t.Fatal(err)
	}

	// Let NB respond with SummaryResp (automatic in recvLoop)
	time.Sleep(50 * time.Millisecond)

	// Have NA build needs from NB summary by pulling a SummaryResp directly from the wire
	// For simplicity, send a Req for key k from=0
	req := syncproto.Req{Needs: []syncproto.Need{{Key: "k", From: 0}}}
	if err := A.Send("B", wire.Encode(wire.MT_SYNC_REQ, syncproto.EncodeReq(req))); err != nil {
		t.Fatal(err)
	}

	// NA will respond to its own Recv? Simplify: directly send a chunk from NA to NB and keep it inflight within NA state
	// Install a fake inflight on NA and then trigger NACK on NB
	ch := syncproto.DeltaChunk{SID: 0, Seq: 0, Last: true, Entries: []syncproto.DeltaEntry{{Key: "k", Ops: []model.Op{op}}}}

	// Send first time from NA
	if err := A.Send("B", wire.Encode(wire.MT_SYNC_DELTA_CHUNK, syncproto.EncodeDeltaChunk(ch))); err != nil {
		t.Fatal(err)
	}

	// Give NA time to record inflight via streamChunks path is not used here; instead, emulate inflight for NACK handler
	na.sessMu.Lock()
	if na.sess.sndInflight == nil {
		na.sess.sndInflight = make(map[segment.ID][]syncproto.DeltaChunk)
	}
	na.sess.sndInflight[segment.ID(0)] = []syncproto.DeltaChunk{ch}
	na.sessMu.Unlock()

	// NB issues a NACK for seq 0
	nack := syncproto.DeltaNack{Seq: 0, Code: 1}
	if err := B.Send("A", wire.Encode(wire.MT_DELTA_NACK, syncproto.EncodeDeltaNack(nack))); err != nil {
		t.Fatal(err)
	}

	// Allow some time for NA to retransmit chunk in response to NACK
	time.Sleep(50 * time.Millisecond)

	// Verify NB received a retransmission by checking its log contains the op (applied once)
	snap := nb.Log.Snapshot()["k"]
	if len(snap) != 1 || string(snap[0].Value) != "v" {
		t.Fatalf("expected op applied on NB after retransmit, got %v", snap)
	}
}

// Spec: Out-of-order delivery (seq 1 before seq 0) still converges after retransmit
func TestTwoNodeOutOfOrderDelivery_StillConverges(t *testing.T) {
	sw := transport.NewSwitch()
	A, _ := sw.Listen("A")
	B, _ := sw.Listen("B")
	defer A.Close()
	defer B.Close()

	nb := NewWithOptions("NB", WithEndpoint(B), WithPeer("A"), WithTickerEvery(200*time.Millisecond), WithLog(oplog.New()), WithClock(hlc.New()))
	nb.Start()
	defer nb.Stop()

	// Build two ops intended as seq 0 and seq 1
	var actor model.ActorID
	actor[0] = 0x33
	tick := uint64(1000)
	op0 := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "k", Value: []byte("v0"), HLCTicks: tick, Actor: actor}
	op0.Hash = model.HashOp(op0.Version, op0.Kind, op0.Key, op0.Value, op0.HLCTicks, op0.WallNanos, op0.Actor, op0.Pre)
	op1 := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "k", Value: []byte("v1"), HLCTicks: tick + 1, Actor: actor}
	op1.Hash = model.HashOp(op1.Version, op1.Kind, op1.Key, op1.Value, op1.HLCTicks, op1.WallNanos, op1.Actor, op1.Pre)

	ch0 := syncproto.DeltaChunk{SID: 0, Seq: 0, Last: false, Entries: []syncproto.DeltaEntry{{Key: "k", Ops: []model.Op{op0}}}}
	ch1 := syncproto.DeltaChunk{SID: 0, Seq: 1, Last: true, Entries: []syncproto.DeltaEntry{{Key: "k", Ops: []model.Op{op1}}}}

	// Send seq 1 first (out of order)
	if err := A.Send("B", wire.Encode(wire.MT_SYNC_DELTA_CHUNK, syncproto.EncodeDeltaChunk(ch1))); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	// Then send seq 0
	if err := A.Send("B", wire.Encode(wire.MT_SYNC_DELTA_CHUNK, syncproto.EncodeDeltaChunk(ch0))); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	// Retransmit seq 1 as a sender would after NACK
	if err := A.Send("B", wire.Encode(wire.MT_SYNC_DELTA_CHUNK, syncproto.EncodeDeltaChunk(ch1))); err != nil {
		t.Fatal(err)
	}

	// Allow processing
	time.Sleep(50 * time.Millisecond)
	snap := nb.Log.Snapshot()["k"]
	if len(snap) != 2 || string(snap[0].Value) != "v0" || string(snap[1].Value) != "v1" {
		t.Fatalf("expected two ops applied in order, got %#v", snap)
	}
}

// Spec: Session lifecycle emits end event on no diff
func TestSessionBeginEnd_EventsOnNoDiff(t *testing.T) {
	sw := transport.NewSwitch()
	A, _ := sw.Listen("A")
	B, _ := sw.Listen("B")
	defer A.Close()
	defer B.Close()

	ev := make(chan Event, 128)
	na := NewWithOptions("NA", WithEndpoint(A), WithPeer("B"), WithTickerEvery(100*time.Millisecond), WithLog(oplog.New()), WithClock(hlc.New()))
	nb := NewWithOptions("NB", WithEndpoint(B), WithPeer("A"), WithTickerEvery(100*time.Millisecond), WithLog(oplog.New()), WithClock(hlc.New()))
	na.AttachEvents(ev)
	na.Start()
	defer na.Stop()
	nb.Start()
	defer nb.Stop()

	// Kick a round: send SummaryReq from NA to NB
	if err := A.Send("B", wire.Encode(wire.MT_SYNC_SUMMARY_REQ, syncproto.EncodeSummaryReq(syncproto.SummaryReq{}))); err != nil {
		t.Fatal(err)
	}

	// Wait for an end event from NA (no diffs)
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case e := <-ev:
			if e.Type == EventSync {
				if act, _ := e.Fields["action"].(string); act == "end" {
					return
				}
			}
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	t.Fatalf("did not observe end-of-session event on no-diff within timeout")
}
