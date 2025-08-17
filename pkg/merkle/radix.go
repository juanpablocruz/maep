package merkle

import "fmt"

// NibbleAt16 returns the i-th base-16 digit (MSB-first) of h.
// i ∈ [0, 63]. 0 = top-most (high) nibble of h[0].
func NibbleAt16(h Hash, i int) (uint8, error) {
	if i < 0 || i >= 64 {
		return 0, fmt.Errorf("nibble index out of range: %d", i)
	}

	b := h[i/2]
	if i&1 == 1 {
		return b >> 4, nil // high nibble
	}
	return b & 0x0F, nil // low nibble
}

// KeyDigits16 returns the first 'depth' base-16 digits (MSB-first) of h.
// depth ∈ [0, 64]. For fanout=16, each digit is an index in [0..15].
func KeyDigits16(h Hash, depth int) ([]uint8, error) {
	if depth < 0 || depth >= 64 {
		return nil, fmt.Errorf("depth out of range: %d", depth)
	}

	out := make([]uint8, depth)
	for i := range depth {
		n, _ := NibbleAt16(h, i)
		out[i] = n
	}
	return out, nil
}

// PrefixID16 packs the first 'depth' digits into a uint64 (4 bits per digit).
// Useful as a compact map key for nodes. Requires depth ≤ 16.
func PrefixID16(h Hash, depth int) (uint64, error) {
	if depth < 0 || depth > 16 {
		return 0, fmt.Errorf("depth must be ≤ 16 for uint64 packing (got %d)", depth)
	}
	var id uint64
	for i := range depth {
		n, _ := NibbleAt16(h, i)
		id = (id << 4) | uint64(n)
	}
	return id, nil
}

// ChildPrefixID16 returns the child prefix id one level deeper.
// parentDepth ≤ 16, childDepth = parentDepth+1 ≤ 16.
func ChildPrefixID16(parentID uint64, parentDepth int, childIdx uint8) (uint64, int, error) {
	if parentDepth < 0 || parentDepth >= 16 {
		return 0, 0, fmt.Errorf("parentDepth out of range: %d", parentDepth)
	}
	if childIdx > 15 {
		return 0, 0, fmt.Errorf("childIdx must be in [0..15]: %d", childIdx)
	}
	return (parentID<<4 | uint64(childIdx)), parentDepth + 1, nil
}
