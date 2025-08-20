// Package metrics implements metrics for the MAEP
package metrics

import "fmt"

type DescMetrics struct {
	Fanout   int
	MaxDepth int
	M        int // total ops n the tree
	Delta    int // ops that differ

	NodesVisited    int // # of Children() responses fetched from the peer
	LeavesTouched   int // prefixes at depth==MaxDepth-1 where children differ
	HashComparisons int // == NodesVisited * Fanout

	AvgDepth     float64
	MaxDepthSeen int

	SummaryBytes int64 // estimated bytes "on wire" for summary responses
	Restarts     int   // #stale-snapshot restarts

	DurationMS float64
}

func (m *DescMetrics) String() string {
	return fmt.Sprintf("Fanout: %d, MaxDepth: %d, M: %d, Delta: %d, NodesVisited: %d, LeavesTouched: %d, HashComparisons: %d, AvgDepth: %.2f, MaxDepthSeen: %d, SummaryBytes: %d, Restarts: %d, DurationMS: %.2f",
		m.Fanout, m.MaxDepth, m.M, m.Delta, m.NodesVisited, m.LeavesTouched, m.HashComparisons, m.AvgDepth, m.MaxDepthSeen, m.SummaryBytes, m.Restarts, m.DurationMS)
}

// ChildSummaryBytes sizes: Hash (32) + Count (8) + LastK (64) = 104 bytes
const ChildSummaryBytes = 32 + 8 + 64
const RespBytesPerNode = 16 * ChildSummaryBytes

type TransferMetrics struct {
	M             int   // local total ops (from root)
	DeltaExact    int   // |B \ A| shipped this session (one-way)
	LeavesTouched int   // number of leaf parents touched (same as summary)
	SummaryBytes  int64 // from summary descent (remote replies only)
	LeafKeysBytes int64 // bytes to fetch remote leaf key lists
	OpsBytes      int64 // bytes to ship missing ops (payload)
	TotalBytes    int64 // sum of the above
}
