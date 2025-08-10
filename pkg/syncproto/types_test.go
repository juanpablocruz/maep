package syncproto

import (
	"bytes"
	"testing"

	"github.com/juanpablocruz/maep/pkg/hlc"
	"github.com/juanpablocruz/maep/pkg/materialize"
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/oplog"
)

// Spec: Summary must include SV/frontier per key (receiver-advertised frontier)
func TestBuildSummaryFromLog_IncludesFrontier(t *testing.T) {
	l := oplog.New()
	clk := hlc.New()
	var actor model.ActorID
	actor[0] = 0xAA
	// two keys with different counts
	for i := 0; i < 3; i++ {
		op := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "A", Value: []byte{byte('a' + i)}, HLCTicks: clk.Now(), Actor: actor}
		op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
		l.Append(op)
	}
	for i := 0; i < 1; i++ {
		op := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "B", Value: []byte{byte('b' + i)}, HLCTicks: clk.Now(), Actor: actor}
		op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
		l.Append(op)
	}
	s := BuildSummaryFromLog(l)
	// leaves reflect materialized snapshot
	view := materialize.Snapshot(l)
	ls := materialize.LeavesFromSnapshot(view)
	if len(s.Leaves) != len(ls) {
		t.Fatalf("leaves mismatch: got %d want %d", len(s.Leaves), len(ls))
	}
	// frontier counts must match CountPerKey()
	counts := l.CountPerKey()
	if len(s.Frontier) != len(counts) {
		t.Fatalf("frontier mismatch len: got %d want %d", len(s.Frontier), len(counts))
	}
	f := make(map[string]uint32, len(s.Frontier))
	for _, it := range s.Frontier {
		f[it.Key] = it.From
	}
	if f["A"] != 3 || f["B"] != 1 {
		t.Fatalf("bad frontier counts: %+v", f)
	}
}

// Spec: DiffForOutbound uses remote frontier and caps by local counts; skips equal leaves
func TestDiffForOutbound_UsesRemoteFrontierAndSkipsEqualLeaves(t *testing.T) {
	local := oplog.New()
	clk := hlc.New()
	var actor model.ActorID
	for i := range actor {
		actor[i] = 0x11
	}

	// Local has two keys A and B; A has 3 ops, B has 2
	add := func(k string, v byte) {
		op := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: k, Value: []byte{v}, HLCTicks: clk.Now(), Actor: actor}
		op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
		local.Append(op)
	}
	add("A", 'a')
	add("A", 'b')
	add("A", 'c')
	add("B", 'x')
	add("B", 'y')

	// Remote summary: same leaves for B (equal hash) and different for A; remote frontier lags on A
	view := materialize.Snapshot(local)
	leaves := materialize.LeavesFromSnapshot(view)
	// We'll pretend remote has same leaf hash for B and different for A by mutating A's expected hash
	var remoteLeaves []LeafSummary
	for _, lf := range leaves {
		if lf.Key == "A" {
			// corrupt to differ
			var h [32]byte
			copy(h[:], bytes.Repeat([]byte{0xFF}, 32))
			remoteLeaves = append(remoteLeaves, LeafSummary{Key: lf.Key, Hash: h})
		} else {
			remoteLeaves = append(remoteLeaves, LeafSummary{Key: lf.Key, Hash: lf.Hash})
		}
	}
	sumRemote := Summary{
		Leaves:   remoteLeaves,
		Frontier: []FrontierItem{{Key: "A", From: 2}, {Key: "B", From: 5}}, // B's From will be capped by local count=2
	}

	req := DiffForOutbound(local, sumRemote)
	if len(req.Needs) == 0 {
		t.Fatalf("expected needs for A (leaf changed)")
	}
	got := make(map[string]uint32)
	for _, n := range req.Needs {
		got[n.Key] = n.From
	}
	// A present with From=2 from remote frontier
	if got["A"] != 2 {
		t.Fatalf("bad From for A: %d", got["A"])
	}
	// B should be skipped because leaves equal
	if _, ok := got["B"]; ok {
		t.Fatalf("did not expect B in needs when leaves equal")
	}
}

// Spec: BuildDeltaFor respects From per key and returns only the suffix
func TestBuildDeltaFor_RespectsFromPerKey(t *testing.T) {
	l := oplog.New()
	clk := hlc.New()
	var actor model.ActorID
	actor[0] = 0x77
	mk := func(k string, v byte) model.Op {
		op := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: k, Value: []byte{v}, HLCTicks: clk.Now(), Actor: actor}
		op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
		return op
	}
	l.Append(mk("X", 'a'))
	l.Append(mk("X", 'b'))
	l.Append(mk("Y", 'q'))
	l.Append(mk("Y", 'r'))
	l.Append(mk("Y", 's'))

	req := Req{Needs: []Need{{Key: "X", From: 1}, {Key: "Y", From: 2}}}
	d := BuildDeltaFor(l, req)
	if len(d.Entries) != 2 {
		t.Fatalf("want 2 entries, got %d", len(d.Entries))
	}
	find := func(k string) []model.Op {
		for _, e := range d.Entries {
			if e.Key == k {
				return e.Ops
			}
		}
		return nil
	}
	if ops := find("X"); len(ops) != 1 || string(ops[0].Value) != string([]byte{'b'}) {
		t.Fatalf("wrong suffix for X: %+v", ops)
	}
	if ops := find("Y"); len(ops) != 1 || string(ops[0].Value) != string([]byte{'s'}) {
		t.Fatalf("wrong suffix for Y: %+v", ops)
	}
}

// Spec: Remote frontier greater than local count must be clamped to local
func TestDiffForOutbound_ClampsRemoteFrontier(t *testing.T) {
	l := oplog.New()
	clk := hlc.New()
	var actor model.ActorID
	actor[0] = 0x21
	// Local has 2 ops for K
	for i := 0; i < 2; i++ {
		op := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "K", Value: []byte{byte('a' + i)}, HLCTicks: clk.Now(), Actor: actor}
		op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
		l.Append(op)
	}
	// Remote advertises frontier From=100 (bogus high)
	view := materialize.Snapshot(l)
	leaves := materialize.LeavesFromSnapshot(view)
	sumRemote := Summary{Leaves: []LeafSummary{{Key: leaves[0].Key, Hash: [32]byte{}}}, Frontier: []FrontierItem{{Key: "K", From: 100}}}
	req := DiffForOutbound(l, sumRemote)
	if len(req.Needs) != 1 || req.Needs[0].Key != "K" {
		t.Fatalf("expected need for K, got %+v", req.Needs)
	}
	if req.Needs[0].From != 2 { // clamped to local count
		t.Fatalf("expected From to clamp to 2, got %d", req.Needs[0].From)
	}
}
