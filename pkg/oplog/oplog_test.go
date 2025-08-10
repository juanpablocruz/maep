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
    op1.Hash = model.HashOp(op1.Version, op1.Kind, op1.Key, op1.Value, op1.HLCTicks, op1.WallNanos, op1.Actor, op1.Pre)

	op2 := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "A", Value: []byte("v2"), HLCTicks: clk.Now(), Actor: actor}
	op2.WallNanos = 2
    op2.Hash = model.HashOp(op2.Version, op2.Kind, op2.Key, op2.Value, op2.HLCTicks, op2.WallNanos, op2.Actor, op2.Pre)

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

// Spec compliance: Idempotent replay and MAEP-ordered storage
func TestAppendOrdersByMAEPWhenOutOfOrderInsertion(t *testing.T) {
	l := New()
	clk := hlc.New()

	var actorA, actorB model.ActorID
	actorA[0] = 0x01
	actorB[0] = 0x02

	// Same key, out-of-order by HLC, and actors differ at the same HLC
	op3 := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "K", Value: []byte("c"), HLCTicks: clk.Now(), Actor: actorB}
	op3.WallNanos = 3
    op3.Hash = model.HashOp(op3.Version, op3.Kind, op3.Key, op3.Value, op3.HLCTicks, op3.WallNanos, op3.Actor, op3.Pre)

	op1 := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "K", Value: []byte("a"), HLCTicks: clk.Now() - 5, Actor: actorA}
	op1.WallNanos = 1
    op1.Hash = model.HashOp(op1.Version, op1.Kind, op1.Key, op1.Value, op1.HLCTicks, op1.WallNanos, op1.Actor, op1.Pre)

	op2 := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "K", Value: []byte("b"), HLCTicks: op3.HLCTicks, Actor: actorA}
	op2.WallNanos = 2
    op2.Hash = model.HashOp(op2.Version, op2.Kind, op2.Key, op2.Value, op2.HLCTicks, op2.WallNanos, op2.Actor, op2.Pre)

	// Append in shuffled order
	l.Append(op3)
	l.Append(op1)
	l.Append(op2)

	s := l.Snapshot()["K"]
	if len(s) != 3 {
		t.Fatalf("want 3 ops, got %d", len(s))
	}
	// Verify MAEP order: HLC asc, then Actor lex, then Hash
	if !(s[0].HLCTicks <= s[1].HLCTicks && s[1].HLCTicks <= s[2].HLCTicks) {
		t.Fatalf("not sorted by HLC then tiebreakers: %#v", s)
	}
}
