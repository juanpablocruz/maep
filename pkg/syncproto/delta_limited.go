package syncproto

import (
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/oplog"
)

// BuildDeltaForLimited builds a delta like BuildDeltaFor, but trims to maxBytes.
func BuildDeltaForLimited(l *oplog.Log, req Req, maxBytes int) (Delta, bool) {
	full := BuildDeltaFor(l, req)

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

	return Delta{Entries: out}, truncated
}
