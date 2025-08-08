package materialize

import (
	"crypto/sha256"

	"github.com/juanpablocruz/maep/pkg/merkle"
)

func LeavesFromSnapshot(view map[string]State) []merkle.Leaf {
	leaves := make([]merkle.Leaf, 0, len(view))
	for k, st := range view {
		h := sha256.New()
		h.Write([]byte(k))
		if st.Present {
			h.Write([]byte{1})
			vh := sha256.Sum256(st.Value)
			h.Write(vh[:])
		} else {
			h.Write([]byte{0})
		}
		var leaf [32]byte
		copy(leaf[:], h.Sum(nil))
		leaves = append(leaves, merkle.Leaf{Key: k, Hash: leaf})
	}
	return leaves
}
