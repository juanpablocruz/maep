package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"slices"

	"github.com/juanpablocruz/maep/pkg/node"
)

type chunkKey struct {
	node string
	seq  uint32
}

type telemetry struct {
	mu sync.Mutex

	// Counters
	chunksSent int64
	chunksRecv int64
	acksSent   int64
	acksRecv   int64
	retrans    int64

	bytesTx int64
	bytesRx int64

	// Per-message-type accounting (indexed by wire MT 0..255)
	msgSentBytes [256]int64
	msgSentCount [256]int64
	msgRecvBytes [256]int64
	msgRecvCount [256]int64

	// Sessions
	sessionsBeg int64
	sessionsEnd map[string]int64 // reason -> count

	// Per-chunk timing
	sendTS   map[chunkKey]time.Time
	rtts     []float64 // seconds
	rttRows  [][]string
	retransN map[chunkKey]int // retransmits per seq

	chunkMeta map[chunkKey]struct {
		entries int
		bytes   int
		last    bool
	}
	chunkRows [][]string

	inflightSeqs map[string]map[uint32]struct{} // node -> set of unacked seqs
	occLastT     map[string]time.Time           // node -> last change time
	occLastC     map[string]int                 // node -> inflight count at last change
	occAreaSec   map[string]float64             // node -> ∫ count dt (seconds*chunks)
	occTotalSec  map[string]float64             // node -> ∑ dt (seconds)
	occMax       map[string]int                 // node -> max inflight seen

	bytesTxNode map[string]int64
	bytesRxNode map[string]int64

	sessActive map[string]bool
	sessBegT   map[string]time.Time
	sessTx0    map[string]int64
	sessRx0    map[string]int64

	sessRatesTx []float64
	sessRatesRx []float64
	sessDurSec  []float64
}

func newTelemetry() *telemetry {
	return &telemetry{
		sessionsEnd: make(map[string]int64),
		sendTS:      make(map[chunkKey]time.Time),
		retransN:    make(map[chunkKey]int),
		chunkMeta: make(map[chunkKey]struct {
			entries, bytes int
			last           bool
		}),
		inflightSeqs: make(map[string]map[uint32]struct{}),
		occLastT:     make(map[string]time.Time),
		occLastC:     make(map[string]int),
		occAreaSec:   make(map[string]float64),
		occTotalSec:  make(map[string]float64),
		occMax:       make(map[string]int),
		bytesTxNode:  make(map[string]int64),
		bytesRxNode:  make(map[string]int64),
		sessActive:   make(map[string]bool),
		sessBegT:     make(map[string]time.Time),
		sessTx0:      make(map[string]int64),
		sessRx0:      make(map[string]int64),
	}
}

func (t *telemetry) handle(ev node.Event) {
	t.mu.Lock()
	defer t.mu.Unlock()

	switch ev.Type {
	case node.EventWire:
		dir, _ := ev.Fields["dir"].(string)
		nbytes, _ := toInt64(ev.Fields["bytes"])
		if nbytes == 0 {
			nbytes, _ = toInt64(ev.Fields["len"])
		} // UDP uses "len"
		mt64, _ := toInt64(ev.Fields["mt"])
		mt := 0
		if mt64 >= 0 && mt64 <= 255 {
			mt = int(mt64)
		}
		if dir == "->" {
			t.bytesTx += nbytes
			t.bytesTxNode[ev.Node] += nbytes
			t.msgSentBytes[mt] += nbytes
			t.msgSentCount[mt]++
		} else {
			t.bytesRx += nbytes
			t.bytesRxNode[ev.Node] += nbytes
			t.msgRecvBytes[mt] += nbytes
			t.msgRecvCount[mt]++
		}

	case node.EventSync:
		act, _ := ev.Fields["action"].(string)
		switch act {
		case "begin":
			t.sessionsBeg++
			t.sessActive[ev.Node] = true
			t.sessBegT[ev.Node] = ev.Time
			t.sessTx0[ev.Node] = t.bytesTxNode[ev.Node]
			t.sessRx0[ev.Node] = t.bytesRxNode[ev.Node]
			// purge any stale inflight from prior session (defensive)
			delete(t.inflightSeqs, ev.Node)
			// start occupancy clock at current level (0 unless we see sends)
			if _, ok := t.occLastT[ev.Node]; !ok {
				t.occLastT[ev.Node] = ev.Time
				t.occLastC[ev.Node] = 0
			}
		case "end":
			if r, _ := ev.Fields["reason"].(string); r != "" {
				t.sessionsEnd[r]++
			} else {
				t.sessionsEnd["(none)"]++
			}
			if t.sessActive[ev.Node] {
				dur := ev.Time.Sub(t.sessBegT[ev.Node]).Seconds()
				if dur > 0 {
					txB := t.bytesTxNode[ev.Node] - t.sessTx0[ev.Node]
					rxB := t.bytesRxNode[ev.Node] - t.sessRx0[ev.Node]
					t.sessRatesTx = append(t.sessRatesTx, float64(txB)/(1024*1024)/dur)
					t.sessRatesRx = append(t.sessRatesRx, float64(rxB)/(1024*1024)/dur)
					t.sessDurSec = append(t.sessDurSec, dur)
				}
			}
			t.sessActive[ev.Node] = false
		}

	case node.EventSendDeltaChunk:
		sequ := toU32(ev.Fields["seq"])
		ck := chunkKey{ev.Node, sequ}
		// track first send only (not retrans)
		if rt, _ := ev.Fields["retrans"].(bool); rt {
			t.retrans++
			t.retransN[ck]++
		} else {
			if _, ok := t.inflightSeqs[ev.Node]; !ok {
				t.inflightSeqs[ev.Node] = make(map[uint32]struct{})
			}
			if _, present := t.inflightSeqs[ev.Node][sequ]; !present {
				t.inflightSeqs[ev.Node][sequ] = struct{}{}
				t.occBump(ev.Node, ev.Time, len(t.inflightSeqs[ev.Node])) // NEW
			}
			if _, ok := t.sendTS[ck]; !ok {
				t.sendTS[ck] = ev.Time
				// capture meta for chunks.csv
				ent := int(toU32(ev.Fields["entries"]))
				byt, _ := toInt64(ev.Fields["bytes"])
				last, _ := ev.Fields["last"].(bool)
				t.chunkMeta[ck] = struct {
					entries int
					bytes   int
					last    bool
				}{entries: ent, bytes: int(byt), last: last}
			}
		}
		t.chunksSent++

	case node.EventAck:
		dir, _ := ev.Fields["dir"].(string)
		switch dir {
		case "send":
			t.acksSent++
		case "recv":
			t.acksRecv++
			sequ := toU32(ev.Fields["seq"])
			ck := chunkKey{node: ev.Node, seq: sequ}
			// RTT capture (existing)
			if ts, ok := t.sendTS[ck]; ok {
				rtt := ev.Time.Sub(ts).Seconds()
				t.rtts = append(t.rtts, rtt)
				t.rttRows = append(t.rttRows, []string{
					ev.Node, fmt.Sprintf("%d", sequ), fmt.Sprintf("%.6f", rtt), fmt.Sprintf("%d", t.retransN[ck]),
				})
				// finalize chunk row
				meta := t.chunkMeta[ck]
				t.chunkRows = append(t.chunkRows, []string{
					ev.Node,
					fmt.Sprintf("%d", sequ),
					ts.UTC().Format(time.RFC3339Nano),
					ev.Time.UTC().Format(time.RFC3339Nano),
					fmt.Sprintf("%.6f", rtt),
					fmt.Sprintf("%d", t.retransN[ck]),
					fmt.Sprintf("%d", meta.entries),
					fmt.Sprintf("%d", meta.bytes),
					fmt.Sprintf("%t", meta.last),
				})
				delete(t.sendTS, ck)
				delete(t.retransN, ck)
				delete(t.chunkMeta, ck)
			}
			// occupancy drop when this seq is newly acked
			if m, ok := t.inflightSeqs[ev.Node]; ok {
				if _, present := m[sequ]; present {
					delete(m, sequ)
					t.occBump(ev.Node, ev.Time, len(m)) // NEW
				}
			}
		}

	case node.EventRecvDeltaChunk:
		t.chunksRecv++

	}
}

func (t *telemetry) writeChunkRTTsCSV(path string) error {
	if len(t.rttRows) == 0 {
		return nil
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{"node", "seq", "rtt_seconds", "retransmits"})
	for _, r := range t.rttRows {
		_ = w.Write(r)
	}
	return nil
}

func (t *telemetry) writeBytesByMsgCSV(path string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{"type_hex", "sent_bytes", "sent_count", "recv_bytes", "recv_count"})
	for i := range 256 {
		sb, sc := t.msgSentBytes[i], t.msgSentCount[i]
		rb, rc := t.msgRecvBytes[i], t.msgRecvCount[i]
		if sb == 0 && sc == 0 && rb == 0 && rc == 0 {
			continue
		}
		_ = w.Write([]string{
			fmt.Sprintf("0x%02X", i),
			fmt.Sprintf("%d", sb),
			fmt.Sprintf("%d", sc),
			fmt.Sprintf("%d", rb),
			fmt.Sprintf("%d", rc),
		})
	}
	return nil
}

func (t *telemetry) writeChunksCSV(path string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.chunkRows) == 0 {
		return nil
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{
		"node", "seq", "sent_ts", "ack_ts", "rtt_seconds", "retransmits", "entries", "bytes", "last",
	})
	for _, r := range t.chunkRows {
		_ = w.Write(r)
	}
	return nil
}

func (t *telemetry) statsLines() (counts, rtt, ses, occ, thr string) {
	// counts + rtt (unchanged)
	mean, p50, p95, p99 := math.NaN(), math.NaN(), math.NaN(), math.NaN()
	if len(t.rtts) > 0 {
		mean = meanFloat(t.rtts)
		vals := percentiles(t.rtts, 50, 95, 99)
		if len(vals) >= 3 {
			p50, p95, p99 = vals[0], vals[1], vals[2]
		}
	}
	counts = fmt.Sprintf(
		"Chunks(sent/recv/retrans): %d/%d/%d  ACKs(sent/recv): %d/%d  Bytes(tx/rx): %d/%d",
		t.chunksSent, t.chunksRecv, t.retrans, t.acksSent, t.acksRecv, t.bytesTx, t.bytesRx,
	)
	if len(t.rtts) == 0 {
		rtt = "ChunkRTT(s): n/a"
	} else {
		rtt = fmt.Sprintf("ChunkRTT(s): mean=%.3f p50=%.3f p95=%.3f p99=%.3f samples=%d",
			mean, p50, p95, p99, len(t.rtts))
	}

	// sessions
	if len(t.sessionsEnd) == 0 {
		ses = "SessionEnds: (none)"
	} else {
		type kv struct {
			k string
			v int64
		}
		var rows []kv
		for k, v := range t.sessionsEnd {
			rows = append(rows, kv{k, v})
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].k < rows[j].k })
		buf := "SessionEnds:"
		for _, r := range rows {
			buf += fmt.Sprintf(" %s=%d", r.k, r.v)
		}
		ses = buf
	}

	// occupancy (avg inflight per node, plus max over time)
	var nodeAvgs []float64
	maxInflight := 0
	for n, area := range t.occAreaSec {
		tot := t.occTotalSec[n]
		if tot > 0 {
			nodeAvgs = append(nodeAvgs, area/tot)
		}
	}
	for _, m := range t.occMax {
		if m > maxInflight {
			maxInflight = m
		}
	}
	if len(nodeAvgs) == 0 {
		occ = "Window(inflight): n/a"
	} else {
		avg := meanFloat(nodeAvgs)
		vals := slices.Clone(nodeAvgs)
		sort.Float64s(vals)
		occ = fmt.Sprintf("Window(inflight): avg=%.2f max=%d node_avg[min..p50..max]=%.2f..%.2f..%.2f (nodes=%d)",
			avg, maxInflight, vals[0], vals[len(vals)/2], vals[len(vals)-1], len(nodeAvgs))
	}

	// per-session throughput (MiB/s)
	sessN := len(t.sessRatesTx)
	if sessN == 0 {
		thr = "Throughput(MiB/s): tx=n/a rx=n/a sessions=0"
	} else {
		txMean := meanFloat(t.sessRatesTx)
		rxMean := meanFloat(t.sessRatesRx)
		txPct := percentiles(t.sessRatesTx, 95)
		rxPct := percentiles(t.sessRatesRx, 95)
		thr = fmt.Sprintf("Throughput(MiB/s): tx_mean=%.2f tx_p95=%.2f  rx_mean=%.2f rx_p95=%.2f  sessions=%d",
			txMean, txPct[0], rxMean, rxPct[0], sessN)
	}
	return
}

// helpers

func percentiles(xs []float64, ps ...int) []float64 {
	ys := slices.Clone(xs)
	sort.Float64s(ys)
	out := make([]float64, len(ps))
	for i, p := range ps {
		if len(ys) == 0 {
			out[i] = math.NaN()
			continue
		}
		rank := (float64(p) / 100.0) * float64(len(ys)-1)
		lo := int(math.Floor(rank))
		hi := int(math.Ceil(rank))
		if lo == hi {
			out[i] = ys[lo]
			continue
		}
		frac := rank - float64(lo)
		out[i] = ys[lo]*(1-frac) + ys[hi]*frac
	}
	return out
}

func meanFloat(xs []float64) float64 {
	if len(xs) == 0 {
		return math.NaN()
	}
	var s float64
	for _, v := range xs {
		s += v
	}
	return s / float64(len(xs))
}

func toInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case float64:
		return int64(x), true
	case float32:
		return int64(x), true
	default:
		return 0, false
	}
}

func toU32(v any) uint32 {
	switch x := v.(type) {
	case int:
		return uint32(x)
	case int32:
		return uint32(x)
	case int64:
		return uint32(x)
	case float64:
		return uint32(x)
	case float32:
		return uint32(x)
	default:
		return 0
	}
}

func (t *telemetry) occBump(node string, now time.Time, newCount int) {
	lastT, ok := t.occLastT[node]
	if ok {
		dt := now.Sub(lastT).Seconds()
		if dt > 0 {
			t.occAreaSec[node] += dt * float64(t.occLastC[node])
			t.occTotalSec[node] += dt
		}
	}
	t.occLastT[node] = now
	t.occLastC[node] = newCount
	if newCount > t.occMax[node] {
		t.occMax[node] = newCount
	}
}

func (t *telemetry) finalize(end time.Time) {
	// close out occupancy integrals to 'end'
	for node, lastT := range t.occLastT {
		dt := end.Sub(lastT).Seconds()
		if dt > 0 {
			t.occAreaSec[node] += dt * float64(t.occLastC[node])
			t.occTotalSec[node] += dt
			t.occLastT[node] = end
		}
	}
}
