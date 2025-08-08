package syncproto

import (
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/oplog"
)

// BuildDeltaForLimited builds a delta like BuildDeltaFor, but trims to maxBytes.
// If the first pass yields zero ops (because Need.From >= local count),
// it retries with From=0 for the same keys so convergence can proceed.
func BuildDeltaForLimited(l *oplog.Log, req Req, maxBytes int) (Delta, bool) {
	full := BuildDeltaFor(l, req)

	totalOps := 0
	for _, e := range full.Entries {
		totalOps += len(e.Ops)
	}

	// Fallback: same counts but different histories → send full history for those keys.
	if totalOps == 0 && len(req.Needs) > 0 {
		req0 := Req{Needs: make([]Need, len(req.Needs))}
		for i, n := range req.Needs {
			req0.Needs[i] = Need{Key: n.Key, From: 0}
		}
		full = BuildDeltaFor(l, req0)
		// recompute to see if we now have something
		totalOps = 0
		for _, e := range full.Entries {
			totalOps += len(e.Ops)
		}
	}

	// ---- trim to maxBytes ----
	size := 4 // u32 entry count
	var out []DeltaEntry
	truncated := false

	for _, e := range full.Entries {
		entryOverhead := 2 + len(e.Key) + 4 // u16 keyLen + key + u32 opCount
		addedHeader := false
		ops := make([]model.Op, 0, len(e.Ops))

		for _, op := range e.Ops {
			opSize := 2 + 1 + 8 + 8 + 16 + 32 + 4 + len(op.Value) // 71 + len(value)
			need := opSize
			if !addedHeader {
				need += entryOverhead
			}
			if size+need > maxBytes {
				truncated = true
				break
			}
			if !addedHeader {
				size += entryOverhead
				addedHeader = true
			}
			ops = append(ops, op)
			size += opSize
		}

		if len(ops) > 0 {
			out = append(out, DeltaEntry{Key: e.Key, Ops: ops})
		}
	}

	// Last resort: force 1 op so the requester can progress (assumes sane maxBytes).
	if len(out) == 0 && len(full.Entries) > 0 && len(full.Entries[0].Ops) > 0 {
		out = []DeltaEntry{{Key: full.Entries[0].Key, Ops: full.Entries[0].Ops[:1]}}
		truncated = true
	}

	return Delta{Entries: out}, truncated
}
