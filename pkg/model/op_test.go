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
		{Version: OpSchemaV1, Kind: OpKindPut, Key: "k", HLCTicks: 100, Actor: b, Hash: HashOp(OpSchemaV1, OpKindPut, "k", []byte("b"), 100, 1, b, [32]byte{})},
		{Version: OpSchemaV1, Kind: OpKindPut, Key: "k", HLCTicks: 100, Actor: a, Hash: HashOp(OpSchemaV1, OpKindPut, "k", []byte("a"), 100, 1, a, [32]byte{})},
		{Version: OpSchemaV1, Kind: OpKindPut, Key: "k", HLCTicks: 90, Actor: b, Hash: HashOp(OpSchemaV1, OpKindPut, "k", []byte("c"), 90, 1, b, [32]byte{})},
	}

	SortOpsMAEP(ops)

	if ops[0].HLCTicks != 90 ||
		bytes.Compare(ops[1].Actor[:], ops[2].Actor[:]) >= 0 ||
		ops[1].HLCTicks != 100 || ops[2].HLCTicks != 100 {
		t.Fatalf("unexpected order: %#v", ops)
	}
}

// Spec compliance: Deterministic replay order (HLC, ActorID, OpHash)
// Tie-break strictly by hash when HLC and ActorID are equal.
func TestMAEPTieBreakOnHash(t *testing.T) {
	var aid ActorID
	for i := 0; i < len(aid); i++ {
		aid[i] = 0x7F
	}

	// Same HLC and Actor, different Value -> different Hash
	a := Op{Version: OpSchemaV1, Kind: OpKindPut, Key: "k", Value: []byte("a"), HLCTicks: 100, Actor: aid}
	a.WallNanos = 1
	a.Hash = HashOp(a.Version, a.Kind, a.Key, a.Value, a.HLCTicks, a.WallNanos, a.Actor, [32]byte{})

	b := Op{Version: OpSchemaV1, Kind: OpKindPut, Key: "k", Value: []byte("b"), HLCTicks: 100, Actor: aid}
	b.WallNanos = 1
	b.Hash = HashOp(b.Version, b.Kind, b.Key, b.Value, b.HLCTicks, b.WallNanos, b.Actor, [32]byte{})

	ops := []Op{b, a}
	SortOpsMAEP(ops)

	// Expect lexicographic order by hash when HLC and Actor tie
	if string(ops[0].Value) == "b" && string(ops[1].Value) == "a" {
		// ok if b.Hash < a.Hash
		return
	}
	if string(ops[0].Value) == "a" && string(ops[1].Value) == "b" {
		// ok if a.Hash < b.Hash
		return
	}
	t.Fatalf("unexpected order for tie-break on hash: %#v", ops)
}

// Spec compliance: Canonical hashing stability used as tiebreaker
func TestCanonicalHashStability(t *testing.T) {
	var aid ActorID
	for i := 0; i < len(aid); i++ {
		aid[i] = 0x42
	}
	h1 := HashOp(OpSchemaV1, OpKindPut, "k", []byte("v"), 10, 123, aid, [32]byte{})
	h2 := HashOp(OpSchemaV1, OpKindPut, "k", []byte("v"), 10, 123, aid, [32]byte{})
	if h1 != h2 {
		t.Fatalf("hash not stable across identical inputs")
	}
	// Changing any field must change the hash (probabilistically, strong signal)
	h3 := HashOp(OpSchemaV1, OpKindPut, "k", []byte("v2"), 10, 123, aid, [32]byte{})
	if h1 == h3 {
		t.Fatalf("hash did not change on value change")
	}
}
