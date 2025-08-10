package materialize

import (
	"bytes"
	"testing"

	"github.com/juanpablocruz/maep/pkg/hlc"
	"github.com/juanpablocruz/maep/pkg/merkle"
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/oplog"
)

func TestSnapshotToMerkleLeavesAndRoot(t *testing.T) {
	l := oplog.New()
	clk := hlc.New()

	var actor model.ActorID
	copy(actor[:], bytes.Repeat([]byte{0x34}, 16))

	// Key A: Put "v1"
	op := model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "A", Value: []byte("v1"), HLCTicks: clk.Now(), Actor: actor}
	op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
	l.Append(op)

	// Key B: Put "v2"
	op = model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "B", Value: []byte("v2"), HLCTicks: clk.Now(), Actor: actor}
	op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
	l.Append(op)

	// Build root #1
	view1 := Snapshot(l)
	leaves1 := LeavesFromSnapshot(view1)
	root1 := merkle.Build(leaves1)

	// Change A to "v3" -> root must change
	op = model.Op{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "A", Value: []byte("v3"), HLCTicks: clk.Now(), Actor: actor}
	op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
	l.Append(op)

	view2 := Snapshot(l)
	leaves2 := LeavesFromSnapshot(view2)
	root2 := merkle.Build(leaves2)

	if root1 == root2 {
		t.Fatalf("Merkle root should change after visible state change")
	}

	// Delete B -> root must change again
	op = model.Op{Version: model.OpSchemaV1, Kind: model.OpKindDel, Key: "B", HLCTicks: clk.Now(), Actor: actor}
	op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
	l.Append(op)

	view3 := Snapshot(l)
	leaves3 := LeavesFromSnapshot(view3)
	root3 := merkle.Build(leaves3)

	if root2 == root3 {
		t.Fatalf("Merkle root should change after delete")
	}
}
