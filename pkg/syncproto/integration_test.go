package syncproto

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/juanpablocruz/maep/pkg/hlc"
	"github.com/juanpablocruz/maep/pkg/materialize"
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/oplog"
	"github.com/juanpablocruz/maep/pkg/transport"
	"github.com/juanpablocruz/maep/pkg/wire"
)

func TestTransportRoundtrip(t *testing.T) {

	sw := transport.NewSwitch()
	a, err := sw.Listen("A")
	if err != nil {
		t.Fatal(err)
	}
	b, err := sw.Listen("B")
	if err != nil {
		t.Fatal(err)
	}

	defer a.Close()
	defer b.Close()

	// Build A's log
	A := oplog.New()
	B := oplog.New()
	clk := hlc.New()
	var actor model.ActorID
	copy(actor[:], bytes.Repeat([]byte{0xAA}, 16))

	put := func(key, val string) {
		op := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: key, Value: []byte(val), HLCTicks: clk.Now(), Actor: actor}
		op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
		A.Append(op)
	}
	del := func(key string) {
		op := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindDel, Key: key, HLCTicks: clk.Now(), Actor: actor}
		op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
		A.Append(op)
	}

	put("A", "v1")
	put("B", "v2")
	del("B")
	put("C", "v3")

	// A -> SUMMARY_REQ -> B
	root := materialize.LeavesFromSnapshot(materialize.Snapshot(A))
	_ = root // not used directly; send root via SummaryReq with dummy zero for now
	if err := a.Send("B", wire.Encode(wire.MT_SYNC_SUMMARY_REQ, EncodeSummaryReq(SummaryReq{}))); err != nil {
		t.Fatal(err)
	}

	// B receives summary, replies with REQ
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	frame, ok := b.Recv(ctx)
	if !ok {
		t.Fatal("no frame")
	}
	mt, payload, err := wire.Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	if mt != wire.MT_SYNC_SUMMARY_REQ {
		t.Fatalf("unexpected mt: %d", mt)
	}
	if _, err := DecodeSummaryReq(payload); err != nil {
		t.Fatal(err)
	}
	// B replies with SummaryResp (its summary + frontier)
	sumB := BuildSummaryFromLog(B)
	if err := b.Send("A", wire.Encode(wire.MT_SYNC_SUMMARY_RESP, EncodeSummary(sumB))); err != nil {
		t.Fatal(err)
	}
	// A receives SummaryResp, builds request using DiffForOutbound
	frame, ok = a.Recv(ctx)
	if !ok {
		t.Fatal("no frame 2")
	}
	mt, payload, err = wire.Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	if mt != wire.MT_SYNC_SUMMARY_RESP {
		t.Fatalf("unexpected mt: %d", mt)
	}
	remoteSum, err := DecodeSummary(payload)
	if err != nil {
		t.Fatal(err)
	}
	req := DiffForOutbound(A, remoteSum)
	if err := b.Send("A", wire.Encode(wire.MT_SYNC_REQ, EncodeReq(req))); err != nil {
		t.Fatal(err)
	}

	// A receives REQ, replies with DELTA
	frame, ok = a.Recv(ctx)
	if !ok {
		t.Fatal("no frame 2")
	}
	mt, payload, err = wire.Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	if mt != wire.MT_SYNC_REQ {
		t.Fatalf("unexpected mt: %d", mt)
	}
	remoteReq, err := DecodeReq(payload)
	if err != nil {
		t.Fatal(err)
	}
	delta := BuildDeltaFor(A, remoteReq)
	if err := a.Send("B", wire.Encode(wire.MT_SYNC_DELTA, EncodeDelta(delta))); err != nil {
		t.Fatal(err)
	}

	// B receives DELTA and applies
	frame, ok = b.Recv(ctx)
	if !ok {
		t.Fatal("no frame 3")
	}
	mt, payload, err = wire.Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	if mt != wire.MT_SYNC_DELTA {
		t.Fatalf("unexpected mt: %d", mt)
	}
	remoteDelta, err := DecodeDelta(payload)
	if err != nil {
		t.Fatal(err)
	}
	ApplyDelta(B, remoteDelta)

	// Verify convergence of visible state
	viewA := materialize.Snapshot(A)
	viewB := materialize.Snapshot(B)
	for k, stA := range viewA {
		stB, ok := viewB[k]
		if !ok || stA.Present != stB.Present || (stA.Present && string(stA.Value) != string(stB.Value)) {
			t.Fatalf("mismatch on %q: A=%#v B=%#v", k, stA, stB)
		}
	}
}

// Spec: SV-bounded sync — receiver advertises frontier and sender streams only suffix
func TestSVBoundedSyncRound(t *testing.T) {
	sw := transport.NewSwitch()
	a, _ := sw.Listen("A")
	b, _ := sw.Listen("B")
	defer a.Close()
	defer b.Close()

	// Local logs
	A := oplog.New()
	B := oplog.New()
	clk := hlc.New()
	var actor model.ActorID
	copy(actor[:], bytes.Repeat([]byte{0xAB}, 16))

	// A has 3 ops for key K
	for i := 0; i < 3; i++ {
		op := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "K", Value: []byte{byte('a' + i)}, HLCTicks: clk.Now(), Actor: actor}
		op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
		A.Append(op)
	}
	// B has 1 op for K
	{
		op := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "K", Value: []byte{'a'}, HLCTicks: clk.Now(), Actor: actor}
		op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
		B.Append(op)
	}

	// A -> SummaryReq, B replies with Summary including Frontier[K]=1
	if err := a.Send("B", wire.Encode(wire.MT_SYNC_SUMMARY_REQ, EncodeSummaryReq(SummaryReq{}))); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	frame, ok := b.Recv(ctx)
	if !ok {
		t.Fatal("no frame")
	}
	mt, payload, err := wire.Decode(frame)
	if err != nil || mt != wire.MT_SYNC_SUMMARY_REQ {
		t.Fatal(err)
	}
	_, _ = DecodeSummaryReq(payload)
	sumB := BuildSummaryFromLog(B)
	if err := b.Send("A", wire.Encode(wire.MT_SYNC_SUMMARY_RESP, EncodeSummary(sumB))); err != nil {
		t.Fatal(err)
	}

	// A receives SummaryResp and builds Req bounded by From=1
	frame, ok = a.Recv(ctx)
	if !ok {
		t.Fatal("no frame 2")
	}
	mt, payload, err = wire.Decode(frame)
	if err != nil || mt != wire.MT_SYNC_SUMMARY_RESP {
		t.Fatal(err)
	}
	remoteSum, err := DecodeSummary(payload)
	if err != nil {
		t.Fatal(err)
	}
	req := DiffForOutbound(A, remoteSum)
	if len(req.Needs) != 1 || req.Needs[0].Key != "K" || req.Needs[0].From != 1 {
		t.Fatalf("expected From=1 for K, got %+v", req.Needs)
	}
	if err := b.Send("A", wire.Encode(wire.MT_SYNC_REQ, EncodeReq(req))); err != nil {
		t.Fatal(err)
	}

	// A replies with Delta containing only suffix (2 ops)
	frame, ok = a.Recv(ctx)
	if !ok {
		t.Fatal("no frame 3")
	}
	mt, payload, err = wire.Decode(frame)
	if err != nil || mt != wire.MT_SYNC_REQ {
		t.Fatal(err)
	}
	remoteReq, err := DecodeReq(payload)
	if err != nil {
		t.Fatal(err)
	}
	delta := BuildDeltaFor(A, remoteReq)
	if len(delta.Entries) != 1 || len(delta.Entries[0].Ops) != 2 {
		t.Fatalf("expected 2 ops in delta suffix, got %+v", delta)
	}
	if err := a.Send("B", wire.Encode(wire.MT_SYNC_DELTA, EncodeDelta(delta))); err != nil {
		t.Fatal(err)
	}

	// B applies and now has 3 ops
	frame, ok = b.Recv(ctx)
	if !ok {
		t.Fatal("no frame 4")
	}
	mt, payload, err = wire.Decode(frame)
	if err != nil || mt != wire.MT_SYNC_DELTA {
		t.Fatal(err)
	}
	remoteDelta, err := DecodeDelta(payload)
	if err != nil {
		t.Fatal(err)
	}
	ApplyDelta(B, remoteDelta)
	if B.CountPerKey()["K"] != 3 {
		t.Fatalf("expected B to have 3 ops after SV-bounded delta")
	}
}
