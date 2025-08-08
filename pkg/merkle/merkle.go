package merkle

import (
	"crypto/sha256"
	"slices"
	"sort"
)

type Leaf struct {
	Key  string
	Hash [32]byte
}

func Build(leaves []Leaf) [32]byte {
	if len(leaves) == 0 {
		return [32]byte{}
	}
	ls := slices.Clone(leaves)
	sort.Slice(ls, func(i, j int) bool { return ls[i].Key < ls[j].Key })

	level := make([][32]byte, len(ls))
	for i := range ls {
		level[i] = ls[i].Hash
	}

	for n := len(level); n > 1; {
		next := make([][32]byte, (n+1)/2)
		for i := 0; i < n; i += 2 {
			if i+1 < n {
				h := sha256.New()
				h.Write(level[i][:])
				h.Write(level[i+1][:])
				copy(next[i/2][:], h.Sum(nil))
			} else {
				next[i/2] = level[i]
			}
		}
		level = next
		n = len(level)
	}
	return level[0]
}
