package syncproto

import (
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/oplog"
)

// BuildDeltaForLimited builds a delta like BuildDeltaFor, but trims it to maxBytes.
// Returns (delta, truncated).
func BuildDeltaForLimited(l *oplog.Log, req Req, maxBytes int) (Delta, bool) {
	// Build the full delta using your existing logic (respects Need.From).
	full := BuildDeltaFor(l, req)

	// Accounting:
	// - top-level entry count: 4 bytes
	// - per-entry header: u16 keyLen + key + u32 opCount = 2 + len(key) + 4
	// - per-op payload:  u16 + u8 + u64 + u64 + 16 + 32 + u32 + len(value) = 71 + len(value)
	size := 4
	var out []DeltaEntry
	truncated := false

	for _, e := range full.Entries {
		entryOverhead := 2 + len(e.Key) + 4
		addedHeader := false
		ops := make([]model.Op, 0, len(e.Ops))

		for _, op := range e.Ops {
			opSize := 71 + len(op.Value)
			need := opSize
			if !addedHeader {
				need += entryOverhead
			}
			if size+need > maxBytes {
				truncated = true
				break // move to next entry
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

	// If nothing fit but we had something to send, force at least one op
	// so the requester can make progress (assumes reasonable maxBytes).
	if len(out) == 0 && len(full.Entries) > 0 && len(full.Entries[0].Ops) > 0 {
		out = []DeltaEntry{{Key: full.Entries[0].Key, Ops: full.Entries[0].Ops[:1]}}
		truncated = true
	}

	return Delta{Entries: out}, truncated
}
