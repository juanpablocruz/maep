package engine

import (
	"bytes"
	"testing"

	"github.com/juanpablocruz/maep/pkg/actor"
	"github.com/juanpablocruz/maep/pkg/hlc"
)

// Test_CORE_OPS_01_ConsistentHash validates operation hash consistency
func Test_CORE_OPS_01_ConsistentHash(t *testing.T) {
	// ID: CORE-OPS-01
	// Target: CORE (Operations)
	// Setup: Create operations with identical content
	// Stimulus: Generate hashes for identical operations
	// Checks: Hashes are consistent and identical
	actorId := actor.ActorID{}
	copy(actorId[:], []byte("actor")[:])

	ts := hlc.HLCTimestamp{
		TS:    1,
		Count: 1,
	}

	o := generateOp(
		WithActorID(actorId),
		WithHLC(ts),
		WithKey([]byte("key1")),
		WithValue([]byte("value1")),
	)

	hash := o.Hash()
	t.Logf("hash: %x", hash)

	o2 := generateOp(
		WithActorID(actorId),
		WithHLC(ts),
		WithKey([]byte("key1")),
		WithValue([]byte("value1")),
	)

	hash2 := o2.Hash()
	t.Logf("hash2: %x", hash2)

	if !bytes.Equal(hash[:], hash2[:]) {
		t.Errorf("hashes should be equal")
	}
}

// Test_CORE_OPS_02_UniqueHash validates operation hash uniqueness
func Test_CORE_OPS_02_UniqueHash(t *testing.T) {
	// ID: CORE-OPS-02
	// Target: CORE (Operations)
	// Setup: Create operations with different content
	// Stimulus: Generate hashes for different operations
	// Checks: Hashes are unique for different operations
	ts := hlc.HLCTimestamp{
		TS:    1,
		Count: 1,
	}
	o := generateOp(
		WithKey([]byte("key1")),
		WithValue([]byte("value1")),
		WithHLC(ts),
	)

	hash := o.Hash()
	t.Logf("hash: %x", hash)

	o2 := generateOp(
		WithKey([]byte("key2")),
		WithValue([]byte("value2")),
		WithHLC(ts),
	)

	hash2 := o2.Hash()
	t.Logf("hash2: %x", hash2)

	if bytes.Equal(hash[:], hash2[:]) {
		t.Errorf("hashes should be different")
	}
}

// Test_CORE_OPS_03_Decode validates operation decoding functionality
func Test_CORE_OPS_03_Decode(t *testing.T) {
	// ID: CORE-OPS-03
	// Target: CORE (Operations)
	// Setup: Create operation and encode it
	// Stimulus: Decode the encoded operation
	// Checks: Decoded operation matches original
	o := generateOp(
		WithKey([]byte("key1")),
		WithValue([]byte("value1")),
	)

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
	if o.HLC.TS != dec.HLC.TS {
		t.Errorf("ts should be equal")
	}
	if o.HLC.Count != dec.HLC.Count {
		t.Errorf("count should be equal")
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
