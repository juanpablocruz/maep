package materialize

import (
	"bytes"
	"testing"

	"github.com/juanpablocruz/maep/pkg/hlc"
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/oplog"
)

func TestSnapshotLWW(t *testing.T) {
	l := oplog.New()
	clk := hlc.New()

	var actor model.ActorID
	copy(actor[:], bytes.Repeat([]byte{0x11}, 16))

	// Key A: put -> delete (delete should win => absent)
	{
		op := model.Op{
			Version:  model.OpSchemaV1,
			Kind:     model.OpKindPut,
			Key:      "A",
			Value:    []byte("v1"),
			HLCTicks: clk.Now(),
			Actor:    actor,
		}
		op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor)
		l.Append(op)

		op = model.Op{
			Version:  model.OpSchemaV1,
			Kind:     model.OpKindDel,
			Key:      "A",
			HLCTicks: clk.Now(),
			Actor:    actor,
		}
		op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor)
		l.Append(op)
	}

	// Key B: delete -> put (put should win => present=v2)
	{
		op := model.Op{
			Version:  model.OpSchemaV1,
			Kind:     model.OpKindDel,
			Key:      "B",
			HLCTicks: clk.Now(),
			Actor:    actor,
		}
		op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor)
		l.Append(op)

		op = model.Op{
			Version:  model.OpSchemaV1,
			Kind:     model.OpKindPut,
			Key:      "B",
			Value:    []byte("v2"),
			HLCTicks: clk.Now(),
			Actor:    actor,
		}
		op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor)
		l.Append(op)
	}

	view := Snapshot(l)
	if st, ok := view["A"]; !ok || st.Present {
		t.Fatalf("A should be absent, got: %#v", st)
	}
	if st, ok := view["B"]; !ok || !st.Present || string(st.Value) != "v2" {
		t.Fatalf("B should be present=v2, got: %#v", st)
	}
}
