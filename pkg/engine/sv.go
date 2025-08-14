package engine

// Goal: bound what the sender must consider if/when we stream deltas
// SV is a frontier, it bounds deltas later, never changes order.

type Frontier struct {
	Key OpCannonicalKey
	Set bool
}

type SV map[PeerID][]Frontier

type SummaryReq struct {
	Peer   PeerID
	Prefix Prefix
	SV     []Frontier
}

type SummaryResp struct {
	Peer     PeerID
	Root     MerkleHash
	Fanout   uint32
	Depth    uint8
	Children []MerkleHash
	Prefix   Prefix
	Err      string
}

type SummaryPeer interface {
	GetID() PeerID
	SendSummaryReq(SummaryReq) (SummaryResp, error)
}

// DiffChildren Given local vs remote child hashes at a prefix, returns which indexes must be descended
func DiffChildren(local, remote []MerkleHash) (need []int) {
	if len(local) != len(remote) {
		need = make([]int, len(local))
		for i := range local {
			need[i] = i
		}
		return need
	}
	for i := range local {
		if local[i] != remote[i] {
			need = append(need, i)
		}
	}
	return need
}

func eqKids(a, b []MerkleHash) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func writeDigit(buf []byte, pos uint, idx uint32, bpl uint) []byte {
	needBits := (pos + 1) * bpl
	buf = ensureBufferSize(buf, needBits)

	_, byteIdx, off := bitPosition(pos, bpl)
	w := createBitWindow(buf, byteIdx)

	mask, shift := calculateMaskAndShift(off, bpl)
	w &^= mask << shift
	w |= (uint16(idx) & mask) << shift

	writeBitWindow(buf, byteIdx, w)
	return buf
}

func prefixExtend(p Prefix, idx int, fanout uint32) Prefix {
	bpl := bitsPerLevel(fanout)
	q := Prefix{Depth: p.Depth + 1, Path: p.Path}
	q.Path = writeDigit(q.Path, uint(p.Depth), uint32(idx), bpl)
	return q
}

// DescendDiff walk the merkle summary tree from the root, comparing child digests at each prefix
func DescendDiff(getLocal func(Prefix) []MerkleHash,
	getRemote func(Prefix) []MerkleHash,
	fanout uint32, depth uint8) []Prefix {

	var out []Prefix

	stack := []Prefix{{Depth: 0, Path: nil}}

	for len(stack) > 0 {
		p := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		lkids := getLocal(p)
		rkids := getRemote(p)

		if eqKids(lkids, rkids) {
			continue
		}

		if p.Depth+1 == depth {
			for i := range fanout {
				if lkids[i] != rkids[i] {
					out = append(out, prefixExtend(p, int(i), fanout))
				}
			}
			continue
		}

		for i := range fanout {
			if lkids[i] != rkids[i] {
				stack = append(stack, prefixExtend(p, int(i), fanout))
			}
		}
	}

	return out
}
