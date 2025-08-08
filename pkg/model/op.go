package model

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
)

const (
	OpSchemaV1 uint16 = 1

	OpKindPut uint8 = 1 // insert/replace
	OpKindDel uint8 = 2 // tombstone
)

// ActorID is a compact writer identity (16 bytes)
type ActorID [16]byte

// Op is an immutable operation appended to a key's log
type Op struct {
	Version   uint16
	Kind      uint8
	Key       string
	Value     []byte
	HLCTicks  uint64
	WallNanos int64
	Actor     ActorID
	Hash      [32]byte
}

// Compute the deterministic hash used as a tie-breaker in ordering
func HashOp(version uint16, kind uint8, key string, value []byte, hlc uint64, wall int64, actor ActorID) [32]byte {
	h := sha256.New()

	var vbuf [2]byte
	binary.LittleEndian.PutUint16(vbuf[:], version)
	h.Write(vbuf[:])

	h.Write([]byte{kind})

	h.Write([]byte(key))
	h.Write(value)

	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[:8], hlc)
	binary.LittleEndian.PutUint64(buf[8:], uint64(wall))
	h.Write(buf[:])
	h.Write(actor[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// sort ops by (HLCTicks, ActorID, Hash)
func SortOpsMAEP(ops []Op) {
	sort.Slice(ops, func(i, j int) bool {
		ai, aj := ops[i], ops[j]
		if ai.HLCTicks != aj.HLCTicks {
			return ai.HLCTicks < aj.HLCTicks
		}
		// lexicographical comparison of actor IDs
		for k := range ai.Actor {
			if ai.Actor[k] != aj.Actor[k] {
				return ai.Actor[k] < aj.Actor[k]
			}
		}

		// tie-break on hash
		for k := range ai.Hash {
			if ai.Hash[k] != aj.Hash[k] {
				return ai.Hash[k] < aj.Hash[k]
			}
		}
		return false
	})
}
