package hlc

import (
	"testing"
	"time"
)

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

// TestNowPhysicalComponent ensures that the physical portion of the timestamp
// returned by Now() reflects the current wall clock time. Prior to fixing the
// clock implementation, the wall time was incorrectly truncated which would
// cause the physical component to wrap every few days.
func TestNowPhysicalComponent(t *testing.T) {
	c := New()

	// Capture the wall clock time before and after the call to Now(). The
	// physical component should fall within this range.
	start := time.Now().UnixNano() >> 16
	ts := c.Now()
	end := time.Now().UnixNano() >> 16

	physical := int64(ts >> 16)
	if physical < start || physical > end {
		t.Fatalf("physical component %d not within [%d,%d]", physical, start, end)
	}
}
