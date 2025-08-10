package syncproto

import (
	"bytes"
	"testing"

	"github.com/juanpablocruz/maep/pkg/hlc"
	"github.com/juanpablocruz/maep/pkg/materialize"
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/oplog"
)

func makeActor(b byte) (a model.ActorID) {
	copy(a[:], bytes.Repeat([]byte{b}, 16))
	return
}

func put(l *oplog.Log, clk *hlc.Clock, actor model.ActorID, key, val string) {
	op := model.Op{
		Version:  model.OpSchemaV1,
		Kind:     model.OpKindPut,
		Key:      key,
		Value:    []byte(val),
		HLCTicks: clk.Now(),
		Actor:    actor,
	}
    op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
	l.Append(op)
}

func del(l *oplog.Log, clk *hlc.Clock, actor model.ActorID, key string) {
	op := model.Op{
		Version:  model.OpSchemaV1,
		Kind:     model.OpKindDel,
		Key:      key,
		HLCTicks: clk.Now(),
		Actor:    actor,
	}
    op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
	l.Append(op)
}

func TestPullSyncConvergesEmptyPeer(t *testing.T) {
	// Node A has data, Node B starts empty.
	A := oplog.New()
	B := oplog.New()
	clk := hlc.New()
	actor := makeActor(0xAA)

	put(A, clk, actor, "A", "v1")
	put(A, clk, actor, "B", "v2")
	del(A, clk, actor, "B") // tombstone B
	put(A, clk, actor, "C", "v3")

	// A -> summary -> B decides req -> A builds delta -> B applies
	sum := BuildSummaryFromLog(A)
	req := DiffForReq(B, sum)
	if len(req.Needs) == 0 {
		t.Fatalf("expected some keys to request")
	}
	delta := BuildDeltaFor(A, req)
	ApplyDelta(B, delta)

	// Materialized views should match for keys A sent
	viewA := materialize.Snapshot(A)
	viewB := materialize.Snapshot(B)
	for k, stA := range viewA {
		stB, ok := viewB[k]
		if !ok {
			t.Fatalf("key %q missing on B", k)
		}
		if stA.Present != stB.Present {
			t.Fatalf("presence mismatch for %q: A=%v B=%v", k, stA.Present, stB.Present)
		}
		if stA.Present && string(stA.Value) != string(stB.Value) {
			t.Fatalf("value mismatch for %q: A=%q B=%q", k, string(stA.Value), string(stB.Value))
		}
	}
}

func TestDiffOnlyRequestsChangedKeys(t *testing.T) {
	A := oplog.New()
	B := oplog.New()
	clk := hlc.New()
	actor := makeActor(0xBB)

	put(A, clk, actor, "X", "1")
	put(A, clk, actor, "Y", "1")
	put(B, clk, actor, "X", "1") // B already matches for X (state)

	sum := BuildSummaryFromLog(A)
	req := DiffForReq(B, sum)

	// Only Y should be requested
	if len(req.Needs) != 1 || req.Needs[0].Key != "Y" {
		t.Fatalf("expected only Y to be requested, got %#v", req.Needs)
	}
	// Frontier should be From=0 for Y
	if req.Needs[0].From != 0 {
		t.Fatalf("expected From=0 for Y, got %d", req.Needs[0].From)
	}
}

func TestBuildDeltaUsesSuffix(t *testing.T) {
	A := oplog.New()
	B := oplog.New()
	clk := hlc.New()
	actor := makeActor(0xCC)

	// A has two ops for K
	put(A, clk, actor, "K", "v1")
	put(A, clk, actor, "K", "v2")

	// >>> Instead of B doing a fresh put (which gets a later HLC),
	// copy A's first op to simulate "B already replicated the first op".
	opsA := A.Snapshot()["K"]
	if len(opsA) != 2 {
		t.Fatalf("unexpected setup, got %d ops", len(opsA))
	}
	B.Append(opsA[0])

	// Remote summary says K changed; B will request From=len(B[K])=1
	sum := BuildSummaryFromLog(A)
	req := DiffForReq(B, sum)
	if len(req.Needs) != 1 || req.Needs[0].Key != "K" {
		t.Fatalf("expected K in needs, got %#v", req.Needs)
	}
	if req.Needs[0].From != 1 {
		t.Fatalf("expected From=1, got %d", req.Needs[0].From)
	}

	delta := BuildDeltaFor(A, req)
	if len(delta.Entries) != 1 || len(delta.Entries[0].Ops) != 1 {
		t.Fatalf("expected exactly 1 missing op, got %#v", delta)
	}

	// Apply and verify B catches up to A (now K=v2)
	ApplyDelta(B, delta)
	viewA := materialize.Snapshot(A)
	viewB := materialize.Snapshot(B)
	if string(viewA["K"].Value) != string(viewB["K"].Value) {
		t.Fatalf("values not equal after suffix apply")
	}
}
