// Package hlc implements an hybric logical clock
package hlc

import (
	"fmt"
	"time"
)

type Clock interface {
	Now() int64
}

type HLC struct {
	clock Clock
	t     HLCTimestamp
}

// An HLCTimestamp is a timestamp that is used to order operations
// it is stored in Big Endian format
type HLCTimestamp struct {
	TS    int64
	Count int64
}

func (t HLCTimestamp) Less(inc HLCTimestamp) bool {
	return t.TS < inc.TS || (t.TS == inc.TS && t.Count < inc.Count)
}

func (t HLCTimestamp) String() string {
	return fmt.Sprintf("TS=%d Count=%d", t.TS, t.Count)
}

func NewWithPT(pt Clock) *HLC {
	return &HLC{t: HLCTimestamp{}, clock: pt}
}

func NewFromTS(t HLCTimestamp) *HLC {
	return &HLC{t: t, clock: PT{}}
}

func (h *HLC) Now() HLCTimestamp {
	t := h.t
	h.t.TS = h.max(t.TS, h.clock.Now())

	if h.t.TS != t.TS {
		h.t.Count = 0
		return h.t
	}

	h.t.Count++
	return h.t
}

func (h *HLC) Update(inc HLCTimestamp) {
	t := h.t
	h.t.TS = h.max(t.TS, inc.TS, h.clock.Now())

	if h.t.TS == t.TS && inc.TS == t.TS {
		h.t.Count = h.max(t.Count, inc.Count) + 1
	} else if t.TS == h.t.TS {
		h.t.Count++
	} else if h.t.TS == inc.TS {
		h.t.Count = inc.Count + 1
	} else {
		h.t.Count = 0
	}
}

func (h *HLC) max(vals ...int64) int64 {
	if len(vals) == 0 {
		return 0
	}

	m := vals[0]
	for i := range vals {
		if vals[i] > m {
			m = vals[i]
		}
	}
	return m
}

// PT is physical time, it uses time.Now().UnixNano()
type PT struct {
	Seconds bool
}

func (p PT) Now() int64 {
	if p.Seconds {
		return time.Now().Unix()
	}
	return time.Now().UnixNano()
}
