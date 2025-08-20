// Package metrics implements metrics for the MAEP
package metrics

import (
	"fmt"
	"sync"
)

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

func (m *TransferMetrics) String() string {
	return fmt.Sprintf("M: %d, DeltaExact: %d, LeavesTouched: %d, SummaryBytes: %d, LeafKeysBytes: %d, OpsBytes: %d, TotalBytes: %d",
		m.M, m.DeltaExact, m.LeavesTouched, m.SummaryBytes, m.LeafKeysBytes, m.OpsBytes, m.TotalBytes)
}

// MetricsLogger is a generalized logger that can handle different types of metrics
type MetricsLogger struct {
	descCh         chan DescMetrics
	transferCh     chan TransferMetrics
	mtx            sync.RWMutex
	latestDesc     DescMetrics
	latestTransfer TransferMetrics
	done           chan struct{}
	processing     sync.WaitGroup
}

// NewMetricsLogger creates a new generalized metrics logger
func NewMetricsLogger() *MetricsLogger {
	return &MetricsLogger{
		descCh:     make(chan DescMetrics, 10),
		transferCh: make(chan TransferMetrics, 10),
		done:       make(chan struct{}),
	}
}

// Log logs any type of metrics (DescMetrics or TransferMetrics)
func (ml *MetricsLogger) Log(metric any) {
	ml.processing.Add(1)
	defer ml.processing.Done()

	switch m := metric.(type) {
	case DescMetrics:
		ml.mtx.Lock()
		ml.latestDesc = m
		ml.mtx.Unlock()

		// Non-blocking send to avoid deadlocks
		select {
		case ml.descCh <- m:
		default:
			// Channel is full, discard oldest and send new
			select {
			case <-ml.descCh:
			default:
			}
			ml.descCh <- m
		}

	case TransferMetrics:
		ml.mtx.Lock()
		ml.latestTransfer = m
		ml.mtx.Unlock()

		// Non-blocking send to avoid deadlocks
		select {
		case ml.transferCh <- m:
		default:
			// Channel is full, discard oldest and send new
			select {
			case <-ml.transferCh:
			default:
			}
			ml.transferCh <- m
		}

	default:
		// Unknown metric type, do nothing
	}
}

// Start starts the background processing goroutine
func (ml *MetricsLogger) Start() {
	go func() {
		for {
			select {
			case <-ml.done:
				return
			case met := <-ml.descCh:
				ml.mtx.Lock()
				ml.latestDesc = met
				ml.mtx.Unlock()
			case met := <-ml.transferCh:
				ml.mtx.Lock()
				ml.latestTransfer = met
				ml.mtx.Unlock()
			}
		}
	}()
}

// ReadDesc returns the latest DescMetrics
func (ml *MetricsLogger) ReadDesc() DescMetrics {
	ml.mtx.RLock()
	defer ml.mtx.RUnlock()
	return ml.latestDesc
}

// ReadTransfer returns the latest TransferMetrics
func (ml *MetricsLogger) ReadTransfer() TransferMetrics {
	ml.mtx.RLock()
	defer ml.mtx.RUnlock()
	return ml.latestTransfer
}

// Wait waits for all pending metrics to be processed
func (ml *MetricsLogger) Wait() {
	ml.processing.Wait()
}

// Close closes the logger and stops the background goroutine
func (ml *MetricsLogger) Close() {
	close(ml.done)
}
