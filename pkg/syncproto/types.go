package syncproto

import (
	"slices"

	"github.com/juanpablocruz/maep/pkg/materialize"
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/oplog"
)

type Summary struct {
	Leaves []LeafSummary
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
	Seq     uint32
	Last    bool
	Entries []DeltaEntry
	Hash    [32]byte
}

type Ack struct {
	Seq uint32
}

type DeltaNack struct {
	Seq  uint32
	Code uint8
}

// BuildSummaryFromLog -> materialize -> leaves -> summary
func BuildSummaryFromLog(l *oplog.Log) Summary {
	view := materialize.Snapshot(l)
	leaves := materialize.LeavesFromSnapshot(view)
	out := Summary{Leaves: make([]LeafSummary, 0, len(leaves))}
	for _, lf := range leaves {
		out.Leaves = append(out.Leaves, LeafSummary{Key: lf.Key, Hash: lf.Hash})
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

// BuildDeltaFor returns all ops for requested keys.
func BuildDeltaFor(src *oplog.Log, req Req) Delta {
	snap := src.Snapshot()
	entries := make([]DeltaEntry, 0, len(req.Needs))
	for _, need := range req.Needs {
		ops := snap[need.Key]
		from := min(max(int(need.From), 0), len(ops))
		if from < len(ops) {
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
