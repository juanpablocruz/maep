package engine

import (
	"bytes"
	"testing"
)

func generateOp(key string, value string, opType OpType) Op {
	return Op{
		Actor: ActorID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		HLC:   NewHLC(1234567890, 12345),
		Type:  OpType(opType),
		Key:   []byte(key),
		Value: []byte(value),
	}
}

func Test_OPS_ConsistentHash(t *testing.T) {
	o := generateOp("key", "value", OpPut)

	hash := o.Hash()
	t.Logf("hash: %x", hash)

	o2 := generateOp("key", "value", OpPut)

	hash2 := o2.Hash()
	t.Logf("hash2: %x", hash2)

	if !bytes.Equal(hash[:], hash2[:]) {
		t.Errorf("hashes should be equal")
	}
}

func Test_OPS_UniqueHash(t *testing.T) {
	o := generateOp("key", "value", OpPut)

	hash := o.Hash()
	t.Logf("hash: %x", hash)

	o2 := generateOp("key2", "value2", OpDel)

	hash2 := o2.Hash()
	t.Logf("hash2: %x", hash2)

	if bytes.Equal(hash[:], hash2[:]) {
		t.Errorf("hashes should be different")
	}
}

func Test_OPS_Decode(t *testing.T) {
	o := generateOp("key", "value", OpPut)

	enc := o.Encode()
	t.Logf("enc: %x", enc)

	dec, err := DecodeOps(enc)
	if err != nil {
		t.Errorf("error decoding: %v", err)
	}
	t.Logf("dec: %v", dec)

	if !bytes.Equal(o.Actor[:], dec.Actor[:]) {
		t.Errorf("actor ids should be equal")
	}
	if o.HLC.wallns != dec.HLC.wallns {
		t.Errorf("wall_ns should be equal")
	}
	if o.HLC.logical != dec.HLC.logical {
		t.Errorf("logical should be equal")
	}
	if o.Type != dec.Type {
		t.Errorf("type should be equal")
	}
	if !bytes.Equal(o.Key, dec.Key) {
		t.Errorf("keys should be equal")
	}
	if !bytes.Equal(o.Value, dec.Value) {
		t.Errorf("values should be equal")
	}
}
