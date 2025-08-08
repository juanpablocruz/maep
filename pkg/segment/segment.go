package segment

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/juanpablocruz/maep/pkg/materialize"
	"github.com/juanpablocruz/maep/pkg/merkle"
)

type ID = uint16

// Deterministic segment id from key: first 2 bytes of sha256(key) (big-endian)
func ForKey(key string) ID {
	sum := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint16(sum[0:2])
}

// GroupLeaves groups merkle leaves by segment.
func GroupLeaves(leaves []merkle.Leaf) map[ID][]merkle.Leaf {
	out := make(map[ID][]merkle.Leaf, 128)
	for _, lf := range leaves {
		out[ForKey(lf.Key)] = append(out[ForKey(lf.Key)], lf)
	}
	return out
}

// RootsBySegment builds a root per segment from a materialized view.
func RootsBySegment(view map[string]materialize.State) map[ID][32]byte {
	leaves := materialize.LeavesFromSnapshot(view)
	bySeg := GroupLeaves(leaves)
	roots := make(map[ID][32]byte, len(bySeg))
	for sid, ls := range bySeg {
		roots[sid] = merkle.Build(ls)
	}
	return roots
}
