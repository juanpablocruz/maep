package syncproto

import (
	"sort"

	"github.com/juanpablocruz/maep/pkg/materialize"
	"github.com/juanpablocruz/maep/pkg/oplog"
	"github.com/juanpablocruz/maep/pkg/segment"
)

// SegAd is a compact advertisement of per-segment roots (and key counts).
type SegAd struct {
	Items []SegAdItem
}

type SegAdItem struct {
	SID   segment.ID
	Root  [32]byte
	Count uint32 // number of keys in this segment's visible state
}

// BuildSegAdFromLog builds a segment advertisement from the current visible state.
func BuildSegAdFromLog(l *oplog.Log) SegAd {
	view := materialize.Snapshot(l)
	roots := segment.RootsBySegment(view)

	// counts per segment
	counts := map[segment.ID]uint32{}
	for k, st := range view {
		if !st.Present {
			continue
		}
		counts[segment.ForKey(k)]++
	}

	items := make([]SegAdItem, 0, len(roots))
	for sid, r := range roots {
		items = append(items, SegAdItem{SID: sid, Root: r, Count: counts[sid]})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].SID < items[j].SID })
	return SegAd{Items: items}
}

// ChangedSegments returns the set of SIDs whose roots differ vs the remote ad.
func ChangedSegments(l *oplog.Log, ad SegAd) map[segment.ID]struct{} {
	view := materialize.Snapshot(l)
	local := segment.RootsBySegment(view)
	out := make(map[segment.ID]struct{})
	for _, it := range ad.Items {
		if local[it.SID] != it.Root {
			out[it.SID] = struct{}{}
		}
	}
	return out
}

func NeedsForSegments(l *oplog.Log, sids map[segment.ID]struct{}) []Need {
	view := materialize.Snapshot(l)
	counts := l.CountPerKey()

	needs := make([]Need, 0, len(view))
	for k, st := range view {
		if !st.Present {
			continue
		}
		if _, ok := sids[segment.ForKey(k)]; !ok {
			continue
		}
		from := uint32(0)
		if c, ok := counts[k]; ok && c > 0 {
			if c > int(^uint32(0)) {
				from = ^uint32(0)
			} else {
				from = uint32(c)
			}
		}
		needs = append(needs, Need{Key: k, From: from})
	}
	sort.Slice(needs, func(i, j int) bool { return needs[i].Key < needs[j].Key })
	return needs
}

// DiffForReqFromSegAd: now returns Req{Needs: ...}
func DiffForReqFromSegAd(l *oplog.Log, ad SegAd) Req {
	diff := ChangedSegments(l, ad)
	if len(diff) == 0 {
		return Req{}
	}
	return Req{Needs: NeedsForSegments(l, diff)}
}
