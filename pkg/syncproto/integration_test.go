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
		op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor)
		A.Append(op)
	}
	del := func(key string) {
		op := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindDel, Key: key, HLCTicks: clk.Now(), Actor: actor}
		op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor)
		A.Append(op)
	}

	put("A", "v1")
	put("B", "v2")
	del("B")
	put("C", "v3")

	// A -> SUMMARY -> B
	sum := BuildSummaryFromLog(A)
	if err := a.Send("B", wire.Encode(wire.MT_SYNC_SUMMARY, EncodeSummary(sum))); err != nil {
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
	if mt != wire.MT_SYNC_SUMMARY {
		t.Fatalf("unexpected mt: %d", mt)
	}
	remoteSum, err := DecodeSummary(payload)
	if err != nil {
		t.Fatal(err)
	}
	req := DiffForReq(B, remoteSum)
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
