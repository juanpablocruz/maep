package hlc

import (
	"sync"
	"time"
)

type Clock struct {
	mu       sync.Mutex
	lastWall int64
	logical  uint32
}

func New() *Clock {
	return &Clock{}
}

func (c *Clock) Now() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	wall := time.Now().UnixNano() >> 16
	switch {
	case wall < c.lastWall:
		c.logical++
	case wall == c.lastWall:
		c.logical++
	default:
		c.lastWall = wall
		c.logical = 0
	}

	return (uint64(c.lastWall) << 16) | (uint64(c.logical) & 0xFFFF)
}

func (c *Clock) Merge(remote uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	local := (uint64(c.lastWall) << 16) | (uint64(c.logical) & 0xFFFF)
	nowTick := uint64(time.Now().UnixNano()) >> 16

	base := max(remote, max(nowTick, local))

	merged := base + 1

	c.lastWall = int64(merged >> 16)
	c.logical = uint32(merged & 0xFFFF)
	return merged
}
