package syncproto

import (
	"slices"

	"github.com/juanpablocruz/maep/pkg/materialize"
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/oplog"
	"github.com/juanpablocruz/maep/pkg/segment"
)

type Summary struct {
	Root   [32]byte
	Leaves []LeafSummary
	// Frontier advertises the receiver's known frontier per key (SV semantics).
	// Not a vector clock; used to bound deltas.
	Frontier []FrontierItem
	// SegFrontier advertises per-segment watermarks (segment root + visible key count).
	SegFrontier []SegFrontierItem
}

// SummaryReq carries the sender's current root so the receiver can reply with SV/frontier.
type SummaryReq struct {
	Root [32]byte
}

type LeafSummary struct {
	Key  string
	Hash [32]byte
}

type Need struct {
	Key  string
	From uint32 // number of ops I already have for this key
}

type Req struct {
	Needs []Need
}

// Delta carries the ops for requested keys (full set for now).
type Delta struct {
	Entries []DeltaEntry
}

type DeltaEntry struct {
	Key string
	Ops []model.Op
}

type DeltaChunk struct {
	SID     segment.ID
	Seq     uint32
	Last    bool
	Entries []DeltaEntry
	Hash    [32]byte
}

type Ack struct {
	SID segment.ID
	Seq uint32
}

type DeltaNack struct {
	Seq  uint32
	Code uint8
}

// FrontierItem advertises a per-key frontier watermark (e.g., number of ops known)
// for SV-style bounding. Implementations may later switch to segment-scoped watermarks.
type FrontierItem struct {
	Key  string
	From uint32
}

// SegFrontierItem summarizes a segment for SV purposes.
type SegFrontierItem struct {
	SID   segment.ID
	Root  [32]byte
	Count uint32
}

// BuildSummaryFromLog -> materialize -> leaves -> summary
func BuildSummaryFromLog(l *oplog.Log) Summary {
	view := materialize.Snapshot(l)
	leaves := materialize.LeavesFromSnapshot(view)
	out := Summary{Leaves: make([]LeafSummary, 0, len(leaves))}
	for _, lf := range leaves {
		out.Leaves = append(out.Leaves, LeafSummary{Key: lf.Key, Hash: lf.Hash})
	}
	// Root is set by the caller (node) during SummaryReq/Resp.
	// Populate a simple per-key frontier using local counts as a watermark.
	counts := l.CountPerKey()
	out.Frontier = make([]FrontierItem, 0, len(counts))
	for k, c := range counts {
		out.Frontier = append(out.Frontier, FrontierItem{Key: k, From: uint32(c)})
	}
	// Per-segment frontier.
	roots := segment.RootsBySegment(view)
	segCounts := map[segment.ID]uint32{}
	for k, st := range view {
		if !st.Present {
			continue
		}
		segCounts[segment.ForKey(k)]++
	}
	out.SegFrontier = make([]SegFrontierItem, 0, len(roots))
	for sid, r := range roots {
		out.SegFrontier = append(out.SegFrontier, SegFrontierItem{SID: sid, Root: r, Count: segCounts[sid]})
	}
	return out
}

// DiffForReq compares our local leaves vs remote summary and returns keys we need.
// Policy (pull): if remote has a key and our leaf differs (or we don't have it), request it.
func DiffForReq(local *oplog.Log, remote Summary) Req {
	myView := materialize.Snapshot(local)
	myLeaves := materialize.LeavesFromSnapshot(myView)
	myMap := make(map[string][32]byte, len(myLeaves))
	for _, lf := range myLeaves {
		myMap[lf.Key] = lf.Hash
	}
	// current counts
	myCounts := local.CountPerKey()

	var needs []Need
	for _, r := range remote.Leaves {
		if h, ok := myMap[r.Key]; !ok || h != r.Hash {
			needs = append(needs, Need{Key: r.Key, From: uint32(myCounts[r.Key])})
		}
	}
	return Req{Needs: needs}
}

// DiffForOutbound builds Needs for deltas we will send to a peer that advertised
// its visible leaves and SV frontier. For each key where our leaf hash differs,
// we set From to the peer's advertised frontier for that key (or 0 if absent).
func DiffForOutbound(local *oplog.Log, remote Summary) Req {
	myView := materialize.Snapshot(local)
	myLeaves := materialize.LeavesFromSnapshot(myView)
	myMap := make(map[string][32]byte, len(myLeaves))
	for _, lf := range myLeaves {
		myMap[lf.Key] = lf.Hash
	}
	// remote leaves and frontier maps
	remoteLeaves := make(map[string][32]byte, len(remote.Leaves))
	for _, lf := range remote.Leaves {
		remoteLeaves[lf.Key] = lf.Hash
	}
	remoteFrontier := make(map[string]uint32, len(remote.Frontier))
	for _, fi := range remote.Frontier {
		remoteFrontier[fi.Key] = fi.From
	}
	// If segment frontiers present, focus diffs to SIDs whose roots differ
	mySegRoots := segment.RootsBySegment(myView)
	targetSegs := map[segment.ID]struct{}{}
	if len(remote.SegFrontier) > 0 {
		for _, it := range remote.SegFrontier {
			if mySegRoots[it.SID] != it.Root {
				targetSegs[it.SID] = struct{}{}
			}
		}
	}
	// local counts to cap From
	myCounts := local.CountPerKey()

	var needs []Need
	for k, myHash := range myMap {
		if len(targetSegs) > 0 {
			if _, ok := targetSegs[segment.ForKey(k)]; !ok {
				continue
			}
		}
		if rh, ok := remoteLeaves[k]; ok && rh == myHash {
			continue // equal leaf; no delta
		}
		from := remoteFrontier[k]
		if c := myCounts[k]; uint32(c) < from {
			from = uint32(c)
		}
		needs = append(needs, Need{Key: k, From: from})
	}
	return Req{Needs: needs}
}

// BuildDeltaFor returns all ops for requested keys.
func BuildDeltaFor(src *oplog.Log, req Req) Delta {
	snap := src.Snapshot()
	entries := make([]DeltaEntry, 0, len(req.Needs))
	for _, need := range req.Needs {
		ops := snap[need.Key]
		from := min(max(int(need.From), 0), len(ops))
		if from < len(ops) {
			// SV guarantees the receiver has the prefix up to From; stream the suffix only.
			cp := slices.Clone(ops[from:])
			entries = append(entries, DeltaEntry{Key: need.Key, Ops: cp})
		}
	}
	return Delta{Entries: entries}
}

func ApplyDelta(dst *oplog.Log, d Delta) {
	for _, e := range d.Entries {
		for _, op := range e.Ops {
			dst.Append(op)
		}
	}
}
