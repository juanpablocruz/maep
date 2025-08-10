package syncproto

import (
	"testing"

	"github.com/juanpablocruz/maep/pkg/hlc"
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/oplog"
)

func TestBuildDeltaForLimited_NoFallbackWhenUpToDate(t *testing.T) {
	l := oplog.New()
	clk := hlc.New()
	var actor model.ActorID
	actor[0] = 0xAB

	// Two ops for key K
	op1 := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "K", Value: []byte("v1"), HLCTicks: clk.Now(), Actor: actor}
	op1.Hash = model.HashOp(op1.Version, op1.Kind, op1.Key, op1.Value, op1.HLCTicks, op1.WallNanos, op1.Actor)
	l.Append(op1)
	op2 := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "K", Value: []byte("v2"), HLCTicks: clk.Now(), Actor: actor}
	op2.Hash = model.HashOp(op2.Version, op2.Kind, op2.Key, op2.Value, op2.HLCTicks, op2.WallNanos, op2.Actor)
	l.Append(op2)

	// Request from a peer already at From=2 should yield an empty delta (no echo of history)
	req := Req{Needs: []Need{{Key: "K", From: 2}}}
	d, truncated := BuildDeltaForLimited(l, req, 64*1024)
	if truncated {
		t.Fatalf("did not expect truncation")
	}
	if len(d.Entries) != 0 {
		t.Fatalf("expected empty delta, got %#v", d)
	}
}

func TestBuildDeltaForLimited_TruncatesToBudget(t *testing.T) {
	l := oplog.New()
	clk := hlc.New()
	var actor model.ActorID
	actor[0] = 0xCD

	// Build several ops for K
	for i := 0; i < 4; i++ {
		op := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "K", Value: []byte{byte('a' + i)}, HLCTicks: clk.Now(), Actor: actor}
		op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor)
		l.Append(op)
	}
	// Ask from 0 to get all, but set a very tight budget to force truncation
	req := Req{Needs: []Need{{Key: "K", From: 0}}}
	d, truncated := BuildDeltaForLimited(l, req, 120) // small budget
	if !truncated {
		t.Fatalf("expected truncation")
	}
	if len(d.Entries) != 1 || len(d.Entries[0].Ops) < 1 || len(d.Entries[0].Ops) >= 4 {
		t.Fatalf("unexpected trimming result: %+v", d)
	}
}
