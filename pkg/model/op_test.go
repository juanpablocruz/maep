package model

import (
	"bytes"
	"testing"
)

func TestMAEPOrder(t *testing.T) {
	var a, b ActorID
	copy(a[:], bytes.Repeat([]byte{0x01}, 16))
	copy(b[:], bytes.Repeat([]byte{0x02}, 16))

	ops := []Op{
		{Version: OpSchemaV1, Kind: OpKindPut, Key: "k", HLCTicks: 100, Actor: b, Hash: HashOp(OpSchemaV1, OpKindPut, "k", []byte("b"), 100, 1, b)},
		{Version: OpSchemaV1, Kind: OpKindPut, Key: "k", HLCTicks: 100, Actor: a, Hash: HashOp(OpSchemaV1, OpKindPut, "k", []byte("a"), 100, 1, a)},
		{Version: OpSchemaV1, Kind: OpKindPut, Key: "k", HLCTicks: 90, Actor: b, Hash: HashOp(OpSchemaV1, OpKindPut, "k", []byte("c"), 90, 1, b)},
	}

	SortOpsMAEP(ops)

	if ops[0].HLCTicks != 90 ||
		bytes.Compare(ops[1].Actor[:], ops[2].Actor[:]) >= 0 ||
		ops[1].HLCTicks != 100 || ops[2].HLCTicks != 100 {
		t.Fatalf("unexpected order: %#v", ops)
	}
}
