package hlc

import "testing"

func TestMonotonic(t *testing.T) {
	c := New()
	prev := c.Now()
	for range 1000 {
		n := c.Now()
		if n <= prev {
			t.Fatalf("HLC is not monotonic: %d <= %d", n, prev)
		}
		prev = n
	}
}

func TestMerge(t *testing.T) {
	c := New()
	local := c.Now()

	lw := local >> 16
	ll := local & 0xFFFF

	// remote: same physical time, +100 logical (with carry handled)
	delta := uint64(100)
	ll2 := (ll + delta) & 0xFFFF
	carry := (ll + delta) >> 16
	remote := ((lw + carry) << 16) | ll2

	merged := c.Merge(remote)
	if merged <= remote {
		t.Fatalf("merge did not move ahead: merged=%d remote=%d", merged, remote)
	}
}
