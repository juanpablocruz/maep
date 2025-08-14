package engine

import (
	"bytes"
	"testing"
)

func Test_OpLog_Append(t *testing.T) {
	ol := NewOpLog()

	op := generateOp("key", "value", OpPut)

	ol.Append(&op)

	if len(ol.Store) != 1 {
		t.Errorf("store should have 1 entry")
	}
	if len(ol.order) != 1 {
		t.Errorf("order should have 1 entry")
	}
}

func Test_OpLog_Append_KeepOrder(t *testing.T) {
	ol := NewOpLog()

	op := generateOp("key", "value", OpPut)

	ol.Append(&op)

	op2 := generateOp("key2", "value2", OpPut)

	ol.Append(&op2)

	if len(ol.Store) != 2 {
		t.Errorf("store should have 2 entries")
	}
	if len(ol.order) != 2 {
		t.Errorf("order should have 2 entries")
	}

	first := ol.order[0]
	second := ol.order[1]
	op1Hash := op.Hash()
	op2Hash := op2.Hash()

	if !bytes.Equal(first.hash[:], op1Hash[:]) {
		t.Errorf("first hash should be equal")
	}
	if !bytes.Equal(second.hash[:], op2Hash[:]) {
		t.Errorf("second hash should be equal")
	}
}

func Test_OpLog_GetOrdered(t *testing.T) {
	ol := NewOpLog()

	op := generateOp("key", "value", OpPut)
	ol.Append(&op)

	op2 := generateOp("key2", "value2", OpPut)
	ol.Append(&op2)

	var ops []Op
	for _, entry := range ol.GetOrdered() {
		ops = append(ops, *entry)
	}

	if !bytes.Equal(ops[0].Key, []byte("key")) {
		t.Errorf("first op should be key, got %s", string(ops[0].Key))
	}
	if !bytes.Equal(ops[1].Key, []byte("key2")) {
		t.Errorf("second op should be key2, got %s", string(ops[1].Key))
	}
}
