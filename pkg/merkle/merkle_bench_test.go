package merkle_test

import (
	"bytes"
	"math"
	mrand "math/rand"
	"testing"

	"github.com/juanpablocruz/maep/pkg/merkle"
	"github.com/juanpablocruz/maep/pkg/metrics"
)

// ---- add near the top of merkle_bench_test.go ----
type fixedEntry struct{ k merkle.OpHash }

func (f fixedEntry) ComputeHash() merkle.OpHash { return f.k }
func (f fixedEntry) Equals(other merkle.MerkleEntry) bool {
	o, ok := other.(fixedEntry)
	if !ok {
		return false
	}
	return bytes.Equal(f.k[:], o.k[:])
}

var rnd = mrand.New(mrand.NewSource(1))

func makeRandomKeysFast(n int) []merkle.OpHash {
	out := make([]merkle.OpHash, n)
	for i := range n {
		// fill 64 bytes quickly
		for j := range len(out[i]) {
			out[i][j] = byte(rnd.Uint32())
		}
	}
	return out
}

// Make a key whose first len(prefixNibbles) nibbles match exactly
func makeKeyWithPrefix(prefix []uint8) merkle.OpHash {
	var k merkle.OpHash
	for j := range len(k) {
		k[j] = byte(rnd.Uint32())
	}
	for i, nib := range prefix {
		b := i / 2
		if i%2 == 0 { // high nibble
			k[b] = (k[b] & 0x0F) | (nib << 4)
		} else { // low nibble
			k[b] = (k[b] & 0xF0) | (nib & 0x0F)
		}
	}
	return k
}

func appendKeys(m *merkle.Merkle, keys []merkle.OpHash) error {
	for i := range keys {
		if err := m.AppendOp(fixedEntry{k: keys[i]}); err != nil {
			return err
		}
	}
	return nil
}

// sumCounts is the 16-child count sum for a Children(root) reply.
func sumCounts(children []merkle.Summary) uint64 {
	var s uint64
	for i := range 16 {
		s += children[i].Count
	}
	return s
}

// --- The benchmark ---

// Scenarios optimized for faster execution and clearer O(log M) demonstration.
// Using smaller M values and ensuring LeavesTouched=1 for clean analysis.
var benchScenarios = []struct {
	name  string
	M     int // total ops initially on both sides
	Delta int // extra ops on B only (all map to same leaf)
	Depth int // MaxDepth calculated as ceil(log_16(M/4))
}{
	{"M1e3_D10", 1_000, 10, 5},
	{"M3e3_D10", 3_000, 10, 5},
	{"M1e4_D10", 10_000, 10, 6},
	{"M3e4_D10", 30_000, 10, 6},
	{"M1e5_D10", 100_000, 10, 7},
	{"M3e5_D10", 300_000, 10, 7},
}

func Benchmark_SummaryDescent(b *testing.B) {
	for _, sc := range benchScenarios {
		b.Run(sc.name, func(b *testing.B) {
			// --- Setup (not timed) ---
			cfg := merkle.Config{Fanout: 16, MaxDepth: sc.Depth, Hasher: NewTestHasher()}

			mA, err := merkle.New(cfg)
			if err != nil {
				b.Fatal(err)
			}
			mB, err := merkle.New(cfg)
			if err != nil {
				b.Fatal(err)
			}

			// Build identical base on both trees using synthesized keys (fast)
			baseKeys := makeRandomKeysFast(sc.M)
			_ = appendKeys(mA, baseKeys)
			_ = appendKeys(mB, baseKeys)

			// Diverge B by Δ ops pinned to ONE leaf (prefix length = MaxDepth)
			if sc.Delta > 0 {
				pref := make([]uint8, sc.Depth)
				for i := range pref {
					pref[i] = 7
				} // any constant nibble path
				delta := make([]merkle.OpHash, sc.Delta)
				for i := 0; i < sc.Delta; i++ {
					delta[i] = makeKeyWithPrefix(pref)
				}
				_ = appendKeys(mB, delta)
			}

			// Snapshots for the session (stable roots)
			sA := mA.Snapshot()
			sB := mB.Snapshot()

			// Sanity: compute M from the root summaries (use local)
			caRoot, err := sA.Children(merkle.Prefix{Depth: 0, Path: nil})
			if err != nil {
				b.Fatal(err)
			}
			MfromRoot := int(sumCounts(caRoot))
			if MfromRoot == 0 && sc.M > 0 {
				b.Fatalf("unexpected M=0 from root")
			}

			// Remote fetcher = mock transport (call sB.Children)
			fetchRemote := func(p merkle.Prefix) ([]merkle.Summary, error) { return sB.Children(p) }

			// --- Timed section ---
			var last metrics.DescMetrics
			ch := make(chan metrics.DescMetrics, 10)
			logger := func(met any) {
				if descMetrics, ok := met.(metrics.DescMetrics); ok {
					ch <- descMetrics
				}
			}
			b.ResetTimer()

			for i := 0; b.Loop(); i++ {
				_, err := merkle.DiffDescent(sA, sB.Root(), sB.MaxDepth(), fetchRemote, logger)
				if err == merkle.ErrStaleSnapshot {
					// Extremely rare here (no mutation during session). Resnap and retry the same i.
					sA = mA.Snapshot()
					sB = mB.Snapshot()
					i--
					continue
				}
				if err != nil {
					b.Fatalf("DiffDescent error: %v", err)
				}

				last = <-ch
			}
			b.StopTimer()

			// --- Report key metrics (as both logs and custom metrics) ---
			// Custom metrics appear alongside ns/op:
			b.ReportMetric(float64(last.NodesVisited), "nodes/op")
			b.ReportMetric(float64(last.HashComparisons), "hashcmp/op")
			b.ReportMetric(float64(last.LeavesTouched), "leaves/op")
			b.ReportMetric(float64(last.SummaryBytes), "sumbytes/op")
			b.ReportMetric(float64(last.M), "M")
			b.ReportMetric(float64(last.Delta), "DeltaBound")

			// Add normalization metrics for O(log M) analysis
			x := math.Log(float64(last.M)) / math.Log(16) // log_16(M)
			nodesPerLeaf := float64(last.NodesVisited) / math.Max(1, float64(last.LeavesTouched))

			b.ReportMetric(nodesPerLeaf, "nodes/leaf")
			b.ReportMetric(x, "log16M")

			// Log a CSV row you can paste into the paper.
			// DurationMS is already computed inside DiffDescent, but the benchmark also has ns/op.
			b.Logf("CSV,%s,M=%d,DeltaBound=%d,Nodes=%d,Leaves=%d,HashCmp=%d,SummaryKB=%.1f,DurationMS=%.2f",
				sc.name,
				last.M, last.Delta, last.NodesVisited, last.LeavesTouched, last.HashComparisons,
				float64(last.SummaryBytes)/1024.0, last.DurationMS,
			)

			// Log the normalized metrics for O(log M) analysis
			b.Logf("CSV2,%s,log16M=%.3f,nodes/leaf=%.2f,Nodes=%d,Leaves=%d",
				sc.name, x, nodesPerLeaf, last.NodesVisited, last.LeavesTouched)
		})
	}
}

func Benchmark_DeltaBytes(b *testing.B) {
	// scenarios: fix M, vary Δ
	cases := []struct {
		name       string
		M          int
		DeltaExact int
		Depth      int
		AvgOpB     int
	}{
		{"M1e5_D1e1", 100_000, 10, 6, 64},
		{"M1e5_D1e2", 100_000, 100, 6, 64},
		{"M1e5_D1e3", 100_000, 1000, 6, 64},
		{"M1e5_D1e4", 100_000, 10_000, 6, 64},
	}

	for _, sc := range cases {
		b.Run(sc.name, func(b *testing.B) {
			cfg := merkle.Config{Fanout: 16, MaxDepth: sc.Depth, Hasher: NewTestHasher()}
			mA, _ := merkle.New(cfg)
			mB, _ := merkle.New(cfg)

			// Base state: M random keys (fast)
			base := makeRandomKeysFast(sc.M)
			_ = appendKeys(mA, base)
			_ = appendKeys(mB, base)

			// Δ pinned to ONE leaf (prefix = all 7s of length Depth)
			pref := make([]uint8, sc.Depth)
			for i := range pref {
				pref[i] = 7
			}
			deltaKeys := make([]merkle.OpHash, sc.DeltaExact)
			for i := 0; i < sc.DeltaExact; i++ {
				deltaKeys[i] = makeKeyWithPrefix(pref)
			}
			_ = appendKeys(mB, deltaKeys)

			sA := mA.Snapshot().(*merkle.MerkleSnapshot)
			sB := mB.Snapshot().(*merkle.MerkleSnapshot)

			fetchSummary := func(p merkle.Prefix) ([]merkle.Summary, error) { return sB.Children(p) }
			fetchLeaf := func(p merkle.Prefix, c uint8) ([]merkle.OpHash, error) { return sB.LeafOps(p, c) }

			var last metrics.TransferMetrics
			ch := make(chan metrics.TransferMetrics, 10)
			logger := func(tm any) {
				if transferMetrics, ok := tm.(metrics.TransferMetrics); ok {
					ch <- transferMetrics
				}
			}
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				err := merkle.DeltaBytes(sA, sB.Root(), sB.MaxDepth(), fetchSummary, fetchLeaf, sc.AvgOpB, logger)
				if err == merkle.ErrStaleSnapshot {
					sA = mA.Snapshot().(*merkle.MerkleSnapshot)
					sB = mB.Snapshot().(*merkle.MerkleSnapshot)
					i--
					continue
				}
				if err != nil {
					b.Fatalf("DeltaBytes: %v", err)
				}
				last = <-ch
			}
			b.StopTimer()

			// Report useful per-run metrics and log a CSV row:
			b.ReportMetric(float64(last.DeltaExact), "delta/op")
			b.ReportMetric(float64(last.TotalBytes), "bytes/op")
			b.ReportMetric(float64(last.SummaryBytes), "sumbytes/op")
			b.ReportMetric(float64(last.LeafKeysBytes), "leafkeys/op")
			b.ReportMetric(float64(last.OpsBytes), "opsbytes/op")

			kb := func(x int64) float64 { return float64(x) / 1024.0 }
			b.Logf("CSV, %s, M=%d, Δ=%d, Δexact=%d, SummaryKB=%.1f, LeafKeysKB=%.1f, OpsKB=%.1f, TotalKB=%.1f",
				sc.name, last.M, sc.DeltaExact, last.DeltaExact,
				kb(last.SummaryBytes), kb(last.LeafKeysBytes), kb(last.OpsBytes), kb(last.TotalBytes))
		})
	}
}
