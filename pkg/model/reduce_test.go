package model

import (
	"bytes"
	"testing"
)

func TestReduceLWW(t *testing.T) {
	var a ActorID
	copy(a[:], bytes.Repeat([]byte{0xAA}, 16))

	// Two writes, then a delete (later HLCTicks): delete wins => absent
	ops := []Op{
		{Version: OpSchemaV1, Kind: OpKindPut, Key: "A", HLCTicks: 10, Actor: a},
		{Version: OpSchemaV1, Kind: OpKindPut, Key: "A", HLCTicks: 20, Actor: a, Value: []byte("v2")},
		{Version: OpSchemaV1, Kind: OpKindDel, Key: "A", HLCTicks: 30, Actor: a},
	}
	SortOpsMAEP(ops)

	present, _, last := ReduceLWW(ops)
	if present || last.Kind != OpKindDel {
		t.Fatalf("expected tombstone to win, got present=%v lastKind=%d", present, last.Kind)
	}

	// Delete then later put: put wins => present
	ops = []Op{
		{Version: OpSchemaV1, Kind: OpKindDel, Key: "B", HLCTicks: 10, Actor: a},
		{Version: OpSchemaV1, Kind: OpKindPut, Key: "B", HLCTicks: 20, Actor: a, Value: []byte("v3")},
	}
	SortOpsMAEP(ops)

	present, val, last := ReduceLWW(ops)
	if !present || string(val) != "v3" || last.Kind != OpKindPut {
		t.Fatalf("expected put to win, got present=%v val=%q lastKind=%d", present, string(val), last.Kind)
	}
}
