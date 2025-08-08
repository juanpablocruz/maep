package oplog

import (
	"bytes"
	"testing"

	"github.com/juanpablocruz/maep/pkg/hlc"
	"github.com/juanpablocruz/maep/pkg/model"
)

func TestAppendOrderAndIdempotent(t *testing.T) {
	l := New()
	clk := hlc.New()

	var actor model.ActorID
	copy(actor[:], bytes.Repeat([]byte{0xAB}, 16))

	op1 := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "A", Value: []byte("v1"), HLCTicks: clk.Now(), Actor: actor}
	op1.WallNanos = 1
	op1.Hash = model.HashOp(op1.Version, op1.Kind, op1.Key, op1.Value, op1.HLCTicks, op1.WallNanos, op1.Actor)

	op2 := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "A", Value: []byte("v2"), HLCTicks: clk.Now(), Actor: actor}
	op2.WallNanos = 2
	op2.Hash = model.HashOp(op2.Version, op2.Kind, op2.Key, op2.Value, op2.HLCTicks, op2.WallNanos, op2.Actor)

	// Force out-of-order appends
	l.Append(op2)
	l.Append(op1)

	snap := l.Snapshot()["A"]
	if len(snap) != 2 {
		t.Fatalf("want 2 ops, got %d", len(snap))
	}
	if snap[0].HLCTicks > snap[1].HLCTicks {
		t.Fatalf("not sorted by HLC")
	}

	// Idempotency
	l.Append(op1)
	snap = l.Snapshot()["A"]
	if len(snap) != 2 {
		t.Fatalf("idempotency failed, got %d", len(snap))
	}
}
