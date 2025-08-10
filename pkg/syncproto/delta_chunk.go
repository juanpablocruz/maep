package syncproto

// ChunkDelta splits a Delta into a sequence of DeltaChunk whose encoded
// payload stays within maxBytes (best-effort upper bound based on size
// estimation). The final chunk is marked Last=true.
func ChunkDelta(d Delta, maxBytes int) []DeltaChunk {
	if maxBytes <= 0 {
		// defensive: if caller passes 0, produce a single chunk per entry
		maxBytes = 32 * 1024
	}

	seq := uint32(0)
	var out []DeltaChunk
	cur := DeltaChunk{SID: 0, Seq: seq, Last: false, Entries: nil}
	room := maxBytes

	// rough upper bound for an entry when encoded on the wire:
	// keyLen(u16) + key + opCount(u32) + per-op fixed parts + values
	estEntrySize := func(e DeltaEntry) int {
		// entry header
		s := 2 + len(e.Key) + 4
		// op: version(u16) + kind(u8) + hlc(u64) + wall(u64) + actor(16) + hash(32) + vlen(u32) + value
		for _, op := range e.Ops {
			s += 2 + 1 + 8 + 8 + 16 + 32 + 4 + len(op.Value)
		}
		return s
	}

	flush := func() {
		if len(cur.Entries) == 0 {
			return
		}
		out = append(out, cur)
		seq++
		cur = DeltaChunk{SID: cur.SID, Seq: seq}
		room = maxBytes
	}

	for _, e := range d.Entries {
		sz := estEntrySize(e)
		if sz > maxBytes {
			// Single entry too large: emit as its own chunk to avoid stalling.
			flush()
			out = append(out, DeltaChunk{SID: cur.SID, Seq: seq, Entries: []DeltaEntry{e}})
			seq++
			cur = DeltaChunk{SID: cur.SID, Seq: seq}
			room = maxBytes
			continue
		}
		if sz > room {
			flush()
		}
		cur.Entries = append(cur.Entries, e)
		room -= sz
	}
	flush()
	if len(out) > 0 {
		out[len(out)-1].Last = true
	}
	return out
}
