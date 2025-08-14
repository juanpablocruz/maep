package engine

import "testing"

func Test_SV_DescendDiff(t *testing.T) {
	getLocal := func(p Prefix) []MerkleHash {
		return []MerkleHash{}
	}
	getRemote := func(p Prefix) []MerkleHash {
		return []MerkleHash{}
	}
	depth := uint8(2)
	fanout := uint32(2)
	out := DescendDiff(getLocal, getRemote, fanout, depth)
	t.Logf("out: %v", out)

	// TODO: implement
}

func Test_SV_DiffChildren(t *testing.T) {
	local := []MerkleHash{}
	remote := []MerkleHash{}
	out := DiffChildren(local, remote)
	t.Logf("out: %v", out)

	// TODO: implement
}

func Test_prefixExtend(t *testing.T) {
	p := Prefix{Depth: 0, Path: nil}
	p = prefixExtend(p, 2, 16)
	p = prefixExtend(p, 3, 16)

	if p.Path[0] != 0x23 {
		t.Errorf("expected path[0] to be 0x23, got %x", p.Path[0])
	}

	q := Prefix{Depth: 0, Path: nil}
	q = prefixExtend(q, 1, 4)
	q = prefixExtend(q, 2, 4)

	if q.Path[0] != 0x60 {
		t.Errorf("expected path[0] to be 0x60, got %x", q.Path[0])
	}

}
