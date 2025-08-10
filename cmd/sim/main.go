package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/juanpablocruz/maep/pkg/materialize"
	"github.com/juanpablocruz/maep/pkg/merkle"
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/node"
	"github.com/juanpablocruz/maep/pkg/transport"
)

var tele = newTelemetry()

var (
	flNodes         = flag.Int("nodes", 5, "number of nodes in ring")
	flDuration      = flag.Duration("duration", 60*time.Second, "run duration")
	flWriteInterval = flag.Duration("write-interval", 3*time.Second, "mean interval between local writes per node")
	flSyncInterval  = flag.Duration("sync-interval", 7*time.Second, "period between sync attempts")
	flHeartbeat     = flag.Duration("heartbeat", 750*time.Millisecond, "heartbeat period")
	flSuspectTO     = flag.Duration("suspect-timeout", 2250*time.Millisecond, "failure-suspect threshold (>=3*heartbeat)")
	flDeltaChunk    = flag.Int("delta-chunk-bytes", 64*1024, "max bytes per SYNC_DELTA frame")
	flOutDir        = flag.String("out", "out", "output directory")
	flBasePort      = flag.Int("base-port", 9100, "base TCP/UDP port (node i uses base+i)")

	// chaos knobs (uniform for now)
	flLoss   = flag.Float64("loss", 0.0, "drop probability [0..1]")
	flDup    = flag.Float64("dup", 0.0, "dup probability [0..1]")
	flReord  = flag.Float64("reorder", 0.0, "reorder probability [0..1]")
	flDelay  = flag.Duration("delay", 0, "base one-way delay")
	flJitter = flag.Duration("jitter", 0, "jitter (+/-)")

	flQuiesceLast = flag.Duration("quiesce-last", 10*time.Second, "stop writers this long before the end to measure convergence")

	flStopWritesAt  = flag.Duration("stop-writes-at", 30*time.Second, "stop writers at this offset; 0=never")
	flFailurePeriod = flag.Duration("failure-period", 0, "mean time between random link failures (0=off)")
	flRecoveryDelay = flag.Duration("recovery-delay", 5*time.Second, "time a failed link stays down before reconnect")

	flDeltaWindowChunks = flag.Int("delta-window-chunks", 1, "max delta chunks in flight per session")
	flRetransTimeout    = flag.Duration("retrans-timeout", 2*time.Second, "retransmit timeout for unacked chunks")

	flDescent = flag.Bool("descent", true, "use Merkle descent for summaries")

	// diagnostics
	flPrintEvents = flag.Bool("print-events", false, "print selected reconciliation events to stdout")
	flTypes       = flag.String("types", "send_root,descent,send_req,send_delta_chunk,recv_delta_chunk,ack,sync,applied_delta,warn", "comma-separated event types to print")
)

type simNode struct {
	Name     string
	TCP, UDP *transport.ChaosEP
	PeerAddr transport.MemAddr
	Node     *node.Node
	Actor    model.ActorID
}

// syncState holds run-time metrics and timestamps for convergence measurement.
type syncState struct {
	firstSync       time.Time
	roundBeg        map[string]time.Time
	latencies       []time.Duration
	convergedAt     time.Time
	sawUnequal      bool
	writesStoppedAt time.Time
	mu              sync.Mutex
}

func main() {
	flag.Parse()

	if *flNodes < 2 {
		fmt.Println("need at least 2 nodes")
		os.Exit(1)
	}
	if err := os.MkdirAll(*flOutDir, 0o755); err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// bring up endpoints
	sns := make([]*simNode, *flNodes)
	for i := range *flNodes {
		tcp, err := transport.ListenTCP(fmt.Sprintf("127.0.0.1:%d", *flBasePort+i))
		if err != nil {
			panic(err)
		}
		udp, err := transport.ListenUDP(fmt.Sprintf("127.0.0.1:%d", *flBasePort+i))
		if err != nil {
			panic(err)
		}
		sns[i] = &simNode{
			Name: fmt.Sprintf("N%02d", i),
			TCP:  transport.WrapChaos(tcp, transport.ChaosConfig{Up: true}),
			UDP:  transport.WrapChaos(udp, transport.ChaosConfig{Up: true}),
		}
	}

	// chaos defaults
	for _, s := range sns {
		s.TCP.SetLoss(*flLoss)
		s.TCP.SetDup(*flDup)
		s.TCP.SetReorder(*flReord)
		s.TCP.SetBaseDelay(*flDelay)
		s.TCP.SetJitter(*flJitter)
		s.UDP.SetLoss(*flLoss)
		s.UDP.SetDup(*flDup)
		s.UDP.SetReorder(*flReord)
		s.UDP.SetBaseDelay(*flDelay)
		s.UDP.SetJitter(*flJitter)
	}

	// ring peers
	for i := range *flNodes {
		next := (i + 1) % *flNodes
		sns[i].PeerAddr = transport.MemAddr(fmt.Sprintf("127.0.0.1:%d", *flBasePort+next))
	}

	// actor ids
	for i := range *flNodes {
		copy(sns[i].Actor[:], bytes.Repeat([]byte{byte(0xA0 + i)}, 16))
	}

	evOut := make(chan node.Event, 8192)
	var fwdWG sync.WaitGroup

	// spawn nodes
	for i := range *flNodes {
		n := node.NewWithOptions(
			sns[i].Name,
			node.WithEndpoint(sns[i].TCP),
			node.WithPeer(sns[i].PeerAddr),
			node.WithTickerEvery(*flSyncInterval),
		)
		n.AttachHB(sns[i].UDP)
		n.SetDeltaMaxBytes(*flDeltaChunk)
		n.AttachEvents(make(chan node.Event, 1024))
		// allow sim to tune HB/suspect
		n.SetHeartbeatEvery(*flHeartbeat)
		hbK := max(int((*flSuspectTO+*flHeartbeat-1) / *flHeartbeat), 3)
		n.SetSuspectThreshold(hbK)
		n.SetDeltaWindowChunks(*flDeltaWindowChunks)
		n.SetRetransTimeout(*flRetransTimeout)
		n.DescentEnabled = *flDescent

		sns[i].Node = n
		n.Start()

		// forward events into single channel; exit on ctx
		fwdWG.Add(1)
		go func(ch <-chan node.Event) {
			defer fwdWG.Done()
			for {
				select {
				case e, ok := <-ch:
					if !ok {
						return
					}
					select {
					case evOut <- e:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(n.GetEvents())
	}

	// tiny seed
	sns[0].Node.Put("A", []byte("v1"), sns[0].Actor)

	// writers
	writersCtx, stopWriters := context.WithCancel(ctx)
	var wWG sync.WaitGroup
	for i := range *flNodes {
		wWG.Add(1)
		go func(s *simNode) {
			defer wWG.Done()
			writerLoop(writersCtx, s, *flWriteInterval)
		}(sns[i])
	}
	// metrics state (before timers that may reference it)
	ss := &syncState{roundBeg: make(map[string]time.Time)}
	// ensure we remember the time writes stopped for convergence measurement
	if flStopWritesAt != nil && *flStopWritesAt > 0 {
		go func() {
			select {
			case <-time.After(*flStopWritesAt):
				ss.mu.Lock()
				if ss.writesStoppedAt.IsZero() {
					ss.writesStoppedAt = time.Now()
				}
				ss.mu.Unlock()
				stopWriters()
			case <-ctx.Done():
			}
		}()
	}
	if *flFailurePeriod > 0 && *flRecoveryDelay > 0 {
		go flapLoop(ctx, sns, *flFailurePeriod, *flRecoveryDelay)
	}

	if *flQuiesceLast > 0 && *flDuration > *flQuiesceLast {
		time.AfterFunc(*flDuration-*flQuiesceLast, func() {
			ss.mu.Lock()
			if ss.writesStoppedAt.IsZero() {
				ss.writesStoppedAt = time.Now()
			}
			ss.mu.Unlock()
			stopWriters()
		})
	}

	// Convergence watcher: after writers stop, poll roots frequently until equal
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ss.mu.Lock()
				stopped := !ss.writesStoppedAt.IsZero()
				already := !ss.convergedAt.IsZero()
				ss.mu.Unlock()
				if !stopped || already {
					continue
				}
				// compute current roots equality
				buf := make([][32]byte, len(sns))
				allEq := true
				for i, s := range sns {
					view := materialize.Snapshot(s.Node.Log)
					leaves := materialize.LeavesFromSnapshot(view)
					slices.SortFunc(leaves, func(a, b merkle.Leaf) int {
						switch {
						case a.Key < b.Key:
							return -1
						case a.Key > b.Key:
							return 1
						default:
							return 0
						}
					})
					buf[i] = merkle.Build(leaves)
					if i > 0 && buf[i] != buf[0] {
						allEq = false
						break
					}
				}
				if allEq {
					ss.mu.Lock()
					if ss.convergedAt.IsZero() {
						ss.convergedAt = time.Now()
					}
					ss.mu.Unlock()
					return
				}
			}
		}
	}()

	rootsCSV, roundsCSV, sumTxt, closeFiles := mustOpenCSVs(*flOutDir, *flNodes)
	defer closeFiles()

	// metrics (rest handled below)

	// sample roots @1Hz
	rootTickerCtx, rootCancel := context.WithCancel(ctx)
	var rootWG sync.WaitGroup
	rootWG.Add(1)
	go func() {
		defer rootWG.Done()
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()
		start := time.Now()
		buf := make([][32]byte, len(sns))
		for {
			select {
			case <-t.C:
				row := make([]string, 0, 1+len(sns))
				row = append(row, fmt.Sprintf("%.3f", time.Since(start).Seconds()))
				allEq := true
				for i, s := range sns {
					view := materialize.Snapshot(s.Node.Log)
					leaves := materialize.LeavesFromSnapshot(view)

					slices.SortFunc(leaves, func(a, b merkle.Leaf) int {
						switch {
						case a.Key < b.Key:
							return -1
						case a.Key > b.Key:
							return 1
						default:
							return 0
						}
					})
					root := merkle.Build(leaves)
					buf[i] = root
					row = append(row, short(root[:]))
					if i > 0 && buf[i] != buf[0] {
						allEq = false
					}
				}
				_ = rootsCSV.Write(row)
				if allEq {
					ss.mu.Lock()
					// Only record convergence after writers have stopped to measure from a stable point.
					if ss.convergedAt.IsZero() && !ss.writesStoppedAt.IsZero() {
						ss.convergedAt = time.Now()
					}
					ss.mu.Unlock()
				}
				ss.mu.Lock()
				// Track divergence (kept for diagnostics), but do not gate convergence on it.
				if !allEq && !ss.firstSync.IsZero() {
					ss.sawUnequal = true
				}
				// Fallback: if we have not seen a session-begin event for any reason,
				// consider the first observed divergence as the start of the convergence window.
				if !allEq && ss.firstSync.IsZero() {
					ss.firstSync = time.Now()
				}
				ss.mu.Unlock()
			case <-rootTickerCtx.Done():
				return
			}
		}
	}()

	// prepare event-type filter
	typeFilter := map[string]bool{}
	if flTypes != nil && *flTypes != "" {
		for _, t := range strings.Split(*flTypes, ",") {
			typeFilter[strings.TrimSpace(t)] = true
		}
	}

	// consume events; track SYNC begin/end and optionally print
	evCtx, evCancel := context.WithCancel(ctx)
	var evWG sync.WaitGroup
	evWG.Add(1)
	go func() {
		defer evWG.Done()
		for {
			select {
			case e, ok := <-evOut:
				if !ok {
					return
				}
				tele.handle(e)
				if *flPrintEvents {
					et := string(e.Type)
					if len(typeFilter) == 0 || typeFilter[et] {
						// compact one-line render
						fmt.Printf("%s %-6s %-20s %v\n", e.Time.Format(time.RFC3339Nano), e.Node, et, e.Fields)
					}
				}
				if e.Type == node.EventSync {
					act, _ := e.Fields["action"].(string)
					switch act {
					case "begin":
						ss.mu.Lock()
						if ss.firstSync.IsZero() {
							ss.firstSync = e.Time
						}
						ss.roundBeg[e.Node] = e.Time
						ss.mu.Unlock()
					case "end":
						ss.mu.Lock()
						if st, ok := ss.roundBeg[e.Node]; ok {
							d := e.Time.Sub(st)
							ss.latencies = append(ss.latencies, d)
							_ = roundsCSV.Write([]string{e.Node, fmt.Sprintf("%.6f", d.Seconds()), fmt.Sprint(e.Fields["reason"])})
							delete(ss.roundBeg, e.Node)
						}
						ss.mu.Unlock()
					}
				}
			case <-evCtx.Done():
				return
			}
		}
	}()

	half := time.NewTimer(*flDuration / 2)
	go func() {
		<-half.C
		ss.mu.Lock()
		if ss.writesStoppedAt.IsZero() {
			ss.writesStoppedAt = time.Now()
		}
		ss.mu.Unlock()
		stopWriters()
	}()

	// stop on duration or Ctrl-C
	timer := time.NewTimer(*flDuration)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	select {
	case <-timer.C:
	case <-sig:
	}

	// shutdown
	cancel()
	rootCancel()
	evCancel()

	// stop nodes/endpoints
	for _, s := range sns {
		s.Node.Stop()
		s.TCP.Close()
		s.UDP.Close()
	}

	// wait
	wWG.Wait()
	fwdWG.Wait()
	close(evOut)
	rootWG.Wait()
	evWG.Wait()

	// Final convergence check at shutdown (in case last ticker tick missed)
	if ss.convergedAt.IsZero() {
		buf := make([][32]byte, len(sns))
		allEq := true
		for i, s := range sns {
			view := materialize.Snapshot(s.Node.Log)
			leaves := materialize.LeavesFromSnapshot(view)
			slices.SortFunc(leaves, func(a, b merkle.Leaf) int {
				switch {
				case a.Key < b.Key:
					return -1
				case a.Key > b.Key:
					return 1
				default:
					return 0
				}
			})
			buf[i] = merkle.Build(leaves)
			if i > 0 && buf[i] != buf[0] {
				allEq = false
			}
		}
		ss.mu.Lock()
		if allEq && ss.convergedAt.IsZero() && !ss.writesStoppedAt.IsZero() {
			ss.convergedAt = time.Now()
		}
		ss.mu.Unlock()
	}

	// aggregate + write summary
	mean, stdev := meanStdevSeconds(ss.latencies)
	fmt.Fprintf(sumTxt, "Nodes: %d\nDuration: %s\nSync interval: %s\nWrite interval: %s\nHB: %s\nSuspect: %s\nDelta max: %d\n",
		*flNodes, flDuration.String(), flSyncInterval.String(), flWriteInterval.String(), flHeartbeat.String(), flSuspectTO.String(), *flDeltaChunk)
	fmt.Fprintf(sumTxt, "Chaos: loss=%.2f dup=%.2f reorder=%.2f delay=%s jitter=%s\n",
		*flLoss, *flDup, *flReord, flDelay.String(), flJitter.String())
	fmt.Fprintf(sumTxt, "SyncLatency(s): mean=%.3f stdev=%.3f samples=%d\n", mean, stdev, len(ss.latencies))
	ss.mu.Lock()
	base := ss.writesStoppedAt
	if base.IsZero() {
		// If writers never stopped, treat convergence as n/a to avoid misleading 0.000
		base = time.Time{}
	}
	if !ss.convergedAt.IsZero() && !base.IsZero() {
		diff := ss.convergedAt.Sub(base).Seconds()
		if diff < 0 {
			diff = 0
		}
		fmt.Fprintf(sumTxt, "Convergence(s): %.3f\n", diff)
	} else {
		fmt.Fprintf(sumTxt, "Convergence(s): n/a\n")
	}
	ss.mu.Unlock()

	tele.finalize(time.Now())
	mean, stdev = meanStdevSeconds(ss.latencies)
	fmt.Fprintf(sumTxt, "Nodes: %d\nDuration: %s\nSync interval: %s\nWrite interval: %s\nHB: %s\nSuspect: %s\nDelta max: %d\n",
		*flNodes, flDuration.String(), flSyncInterval.String(), flWriteInterval.String(), flHeartbeat.String(), flSuspectTO.String(), *flDeltaChunk)
	fmt.Fprintf(sumTxt, "Chaos: loss=%.2f dup=%.2f reorder=%.2f delay=%s jitter=%s\n",
		*flLoss, *flDup, *flReord, flDelay.String(), flJitter.String())
	fmt.Fprintf(sumTxt, "Delta window: %d  Retrans timeout: %s\n", *flDeltaWindowChunks, flRetransTimeout.String())
	fmt.Fprintf(sumTxt, "SyncLatency(s): mean=%.3f stdev=%.3f samples=%d\n", mean, stdev, len(ss.latencies))
	ss.mu.Lock()
	base2 := ss.writesStoppedAt
	if base2.IsZero() {
		base2 = time.Time{}
	}
	if !ss.convergedAt.IsZero() && !base2.IsZero() {
		diff := ss.convergedAt.Sub(base2).Seconds()
		if diff < 0 {
			diff = 0
		}
		fmt.Fprintf(sumTxt, "Convergence(s): %.3f\n", diff)
	} else {
		fmt.Fprintf(sumTxt, "Convergence(s): n/a\n")
	}
	ss.mu.Unlock()

	counts, rtt, ses, occ, thr := tele.statsLines()
	fmt.Fprintln(sumTxt, counts)
	fmt.Fprintln(sumTxt, rtt)
	fmt.Fprintln(sumTxt, ses)
	fmt.Fprintln(sumTxt, occ)
	fmt.Fprintln(sumTxt, thr)

	// Per-chunk RTTs CSV (already suggested)
	_ = tele.writeChunkRTTsCSV(filepath.Join(*flOutDir, "chunk_rtts.csv"))
	_ = tele.writeBytesByMsgCSV(filepath.Join(*flOutDir, "bytes_by_msg.csv"))
	_ = tele.writeChunksCSV(filepath.Join(*flOutDir, "chunks.csv"))
}

func writerLoop(ctx context.Context, s *simNode, mean time.Duration) {
	keys := []string{"A", "B", "C", "D", "E", "F", "G", "H"}
	lambda := 1.0 / mean.Seconds()
	for {
		// exponential backoff for Poisson process
		sleep := time.Duration(rand.ExpFloat64()/lambda*1e9) * time.Nanosecond
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
			k := keys[rand.Intn(len(keys))]
			v := fmt.Sprintf("v%d", rand.Intn(1000))
			s.Node.Put(k, []byte(v), s.Actor)
		}
	}
}

// flapLoop periodically drops one ring link (i <-> i+1) then restores it.
func flapLoop(ctx context.Context, sns []*simNode, meanPeriod, down time.Duration) {
	if meanPeriod <= 0 || down <= 0 {
		return
	}
	lambda := 1.0 / meanPeriod.Seconds()
	n := len(sns)
	for {
		// next failure time ~ Exp(meanPeriod)
		sleep := time.Duration(rand.ExpFloat64()/lambda*1e9) * time.Nanosecond
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
			i := rand.Intn(n)
			j := (i + 1) % n
			// simulate link down between i and j
			sns[i].Node.SetConnected(false)
			sns[j].Node.SetConnected(false)
			time.Sleep(down)
			sns[i].Node.SetConnected(true)
			sns[j].Node.SetConnected(true)
		}
	}
}

func mustOpenCSVs(dir string, n int) (*csv.Writer, *csv.Writer, *os.File, func()) {
	rootF, err := os.Create(filepath.Join(dir, "roots.csv"))
	if err != nil {
		panic(err)
	}
	roundF, err := os.Create(filepath.Join(dir, "sync_rounds.csv"))
	if err != nil {
		panic(err)
	}
	sumF, err := os.Create(filepath.Join(dir, "summary.txt"))
	if err != nil {
		panic(err)
	}
	rw1 := csv.NewWriter(rootF)
	rw2 := csv.NewWriter(roundF)

	_ = rw1.Write(headerRoots(n))
	_ = rw2.Write([]string{"node", "latency_seconds", "reason"})

	closer := func() {
		rw1.Flush()
		rw2.Flush()
		_ = rootF.Close()
		_ = roundF.Close()
		_ = sumF.Close()
	}
	return rw1, rw2, sumF, closer
}

func headerRoots(n int) []string {
	h := make([]string, 0, 1+n)
	h = append(h, "t_sec")
	for i := range n {
		h = append(h, fmt.Sprintf("N%02d", i))
	}
	return h
}

func short(b []byte) string {
	const hexd = "0123456789abcdef"
	out := make([]byte, 0, 8)
	for i := 0; i < 4 && i < len(b); i++ {
		out = append(out, hexd[b[i]>>4], hexd[b[i]&0x0f])
	}
	return string(out)
}

func meanStdevSeconds(ds []time.Duration) (mean, stdev float64) {
	if len(ds) == 0 {
		return 0, 0
	}
	var sum float64
	secs := make([]float64, len(ds))
	for i, d := range ds {
		secs[i] = d.Seconds()
		sum += secs[i]
	}
	mean = sum / float64(len(secs))
	if len(secs) < 2 {
		return mean, 0
	}
	var s float64
	for _, x := range secs {
		diff := x - mean
		s += diff * diff
	}
	stdev = math.Sqrt(s / float64(len(secs)-1))
	return mean, stdev
}
