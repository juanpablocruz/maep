package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"slices"

	"github.com/juanpablocruz/maep/pkg/hlc"
	"github.com/juanpablocruz/maep/pkg/materialize"
	"github.com/juanpablocruz/maep/pkg/merkle"
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/oplog"
	"github.com/juanpablocruz/maep/pkg/protoport"
	"github.com/juanpablocruz/maep/pkg/segment"
	"github.com/juanpablocruz/maep/pkg/syncproto"
	"github.com/juanpablocruz/maep/pkg/transport"
	"github.com/juanpablocruz/maep/pkg/wire"
)

type syncSession struct {
	id          uint64
	active      bool
	lastRemote  syncproto.Summary
	sndNext     uint32
	sndInflight map[segment.ID][]syncproto.DeltaChunk
	rcvExpect   map[segment.ID]uint32
	ackCh       chan syncproto.Ack
	ctx         context.Context
	cancel      context.CancelFunc
	dqueue      [][]byte
	lastRoot    [32]byte
	haveRoot    bool

	// Client-side bookkeeping for requests awaiting a delta response
	lastReq         syncproto.Req
	awaitingDelta   bool
	repairAttempted bool

	// Summary we most recently advertised to the peer (for SV-bounded checks)
	sentFrontier map[string]uint32
}

type Node struct {
	Name   string
	Peer   transport.MemAddr
	EP     transport.EndpointIF
	HB     transport.EndpointIF
	ms     protoport.Messenger
	hbms   protoport.Messenger
	Log    *oplog.Log
	Clock  *hlc.Clock
	Ticker time.Duration

	Events chan Event

	DeltaMaxBytes     int
	DeltaWindowChunks int
	RetransTimeout    time.Duration

	DescentEnabled bool
	DescentLeafK   int

	kickCh      chan struct{}
	backoffBase time.Duration
	backoffMax  time.Duration

	conn   atomic.Bool
	paused atomic.Bool

	hbEvery time.Duration
	hbMissK int

	lastPong atomic.Int64
	misses   atomic.Int32

	sess   syncSession
	sessMu sync.Mutex

	suspect atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc

	// Persisted per-peer SV (SummaryResp) indexed by peer address
	peerSVMu sync.Mutex
	peerSV   map[string]syncproto.Summary
}

func New(name string, ep transport.EndpointIF, peer transport.MemAddr, tickEvery time.Duration) *Node {
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		Name:              name,
		Peer:              peer,
		EP:                ep,
		Log:               oplog.New(),
		Clock:             hlc.New(),
		Ticker:            tickEvery,
		DeltaMaxBytes:     32 * 1024,
		DeltaWindowChunks: 1,
		RetransTimeout:    2 * time.Second,
		DescentEnabled:    true,
		DescentLeafK:      64,
		kickCh:            make(chan struct{}, 1),
		backoffBase:       500 * time.Millisecond,
		backoffMax:        5 * time.Second,
		hbEvery:           350 * time.Millisecond,
		hbMissK:           6,
		ctx:               ctx, cancel: cancel,
		peerSV: make(map[string]syncproto.Summary),
	}

	n.conn.Store(true)
	n.paused.Store(false)
	// initialize typed messenger for main endpoint
	if ep != nil {
		n.ms = protoport.WireMessenger{EP: ep}
	}
	return n
}

func (n *Node) AttachHB(ep transport.EndpointIF) {
	n.HB = ep
	if ep != nil {
		n.hbms = protoport.WireMessenger{EP: ep}
	}
}

func (n *Node) AttachEvents(ch chan Event) { n.Events = ch }

func (n *Node) Start() {
	// ensure messenger is present
	if n.ms == nil && n.EP != nil {
		n.ms = protoport.WireMessenger{EP: n.EP}
	}
	go n.recvLoop()
	if n.HB != nil {
		go n.recvHBLoop()
	}
	go n.summaryLoop()
	go n.heartbeatLoop()
}

func (n *Node) Stop() { n.cancel() }

func (n *Node) SetConnected(up bool) {
	prev := n.conn.Load()
	n.conn.Store(up)
	if prev != up {
		n.emit(EventConnChange, map[string]any{"up": up})
	}
}
func (n *Node) IsConnected() bool { return n.conn.Load() }

func (n *Node) SetPaused(p bool) {
	prev := n.paused.Load()
	n.paused.Store(p)
	if prev != p {
		n.emit(EventPauseChange, map[string]any{"paused": p})
	}
}

func (n *Node) SetDeltaWindowChunks(w int) {
	if w < 1 {
		w = 1
	}
	n.DeltaWindowChunks = w
}
func (n *Node) SetRetransTimeout(d time.Duration) {
	if d < 100*time.Millisecond {
		d = 100 * time.Millisecond
	}
	n.RetransTimeout = d
}

func (n *Node) SetHeartbeatEvery(d time.Duration) { n.HBSetEvery(d) }
func (n *Node) SetSuspectThreshold(k int)         { n.HBSetMissK(k) }
func (n *Node) SetDeltaMaxBytes(b int)            { n.DeltaMaxBytes = b }
func (n *Node) HBSetEvery(d time.Duration)        { n.hbEvery = d } // or n.hbEvery = d if field is unexported
func (n *Node) HBSetMissK(k int)                  { n.hbMissK = k } // or n.hbMissK = k if unexported

func (n *Node) GetEvents() <-chan Event { return n.Events } // if you prefer not to export field

func (n *Node) IsPaused() bool { return n.paused.Load() }

func (n *Node) Put(key string, val []byte, actor model.ActorID) {
	// set Pre to last known op hash for this key
	var pre [32]byte
	if last, ok := n.Log.LastOp(key); ok {
		pre = last.Hash
	}
	op := model.Op{
		Version:   model.OpSchemaV1,
		Kind:      model.OpKindPut,
		Key:       key,
		Value:     slices.Clone(val),
		HLCTicks:  n.Clock.Now(),
		WallNanos: time.Now().UnixNano(),
		Actor:     actor,
		Pre:       pre,
	}
	op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
	n.Log.Append(op)
	slog.Info("put", "node", n.Name, "key", key, "val", string(val), "hlc", op.HLCTicks)
	n.emit(EventPut, map[string]any{"key": key, "val": string(val), "hlc": op.HLCTicks})
	n.kick()
}

func (n *Node) Delete(key string, actor model.ActorID) {
	var pre [32]byte
	if last, ok := n.Log.LastOp(key); ok {
		pre = last.Hash
	}
	op := model.Op{
		Version:   model.OpSchemaV1,
		Kind:      model.OpKindDel,
		Key:       key,
		HLCTicks:  n.Clock.Now(),
		WallNanos: time.Now().UnixNano(),
		Actor:     actor,
		Pre:       pre,
	}
	op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor, op.Pre)
	n.Log.Append(op)
	slog.Info("del", "node", n.Name, "key", key, "hlc", op.HLCTicks)
	n.emit(EventDel, map[string]any{"key": key, "hlc": op.HLCTicks})
	n.kick()
}

func (n *Node) recvHBLoop() {
	for {
		var (
			from  transport.MemAddr
			frame []byte
			ok    bool
		)

		if fe, okFE := n.HB.(transport.FromEndpoint); okFE {
			from, frame, ok = fe.RecvFrom(n.ctx)
		} else {
			frame, ok = n.HB.Recv(n.ctx)
			from = n.Peer
		}

		if !ok {
			return
		}
		mt, _, err := wire.Decode(frame)
		if err != nil {
			n.emit(EventWarn, map[string]any{"msg": "wire_decode_err_hb", "err": err.Error()})
			continue
		}
		n.emit(EventWire, map[string]any{"proto": "udp", "dir": "<-", "mt": int(mt), "bytes": len(frame), "from": string(from)})
		switch mt {
		case wire.MT_PING:
			n.emit(EventHB, map[string]any{"dir": "<-udp"})
			if err := n.sendHBTo(from, wire.MT_PONG, nil); err != nil {
				n.emit(EventWarn, map[string]any{"msg": "send_pong_err_udp", "err": err.Error()})
			}
		case wire.MT_PONG:
			n.emit(EventHB, map[string]any{"dir": "<-udp"})
			n.onPong()
		default:
			// Ignore non-HB messages on UDP path for now.
		}
	}
}

func (n *Node) summaryLoop() {
	t := time.NewTicker(n.Ticker)
	defer t.Stop()

	var lastRoot [32]byte
	lastSend := time.Time{}
	maxSilence := 3 * n.Ticker

	backoff := n.backoffBase
	nextAllowed := time.Now()

	trySend := func(force bool) {
		// guard: paused or link down
		if n.paused.Load() || !n.conn.Load() || time.Now().Before(nextAllowed) {
			return
		}

		view := materialize.Snapshot(n.Log)
		ls := materialize.LeavesFromSnapshot(view)
		sortLeaves(ls)
		root := merkle.Build(ls)
		leafCount := len(ls)

		if bytes.Equal(root[:], lastRoot[:]) && time.Since(lastSend) < maxSilence && !force {
			return
		}

		n.beginSessionIfNeeded()

		n.emit(EventSendRoot, map[string]any{
			"peer": n.Peer, "leaves": leafCount, "root": fmt.Sprintf("%x", root[:8]),
		})

		// send root via messenger when available
		if n.ms != nil {
			if err := n.ms.Send(n.ctx, n.Peer, protoport.RootMsg{R: syncproto.Root{Hash: root}}); err != nil {
				n.warn("send_root_err", err)
				backoff = minDur(n.backoffMax, maxDur(n.backoffBase, backoff*2))
				nextAllowed = time.Now().Add(jitter(backoff))
				return
			}
		} else {
			if err := n.send(wire.MT_SYNC_ROOT, syncproto.EncodeRoot(syncproto.Root{Hash: root})); err != nil {
				n.warn("send_root_err", err)
				backoff = minDur(n.backoffMax, maxDur(n.backoffBase, backoff*2))
				nextAllowed = time.Now().Add(jitter(backoff))
				return
			}
		}
		backoff = n.backoffBase
		nextAllowed = time.Now()
		lastRoot = root
		lastSend = time.Now()

		if !n.DescentEnabled {
			ad := syncproto.BuildSegAdFromLog(n.Log)
			n.emit(EventSendSegAd, map[string]any{"items": len(ad.Items)})
			if n.ms != nil {
				if err := n.ms.Send(n.ctx, n.Peer, protoport.SegAdMsg{A: ad}); err != nil {
					slog.Warn("send_segad_err", "node", n.Name, "err", err)
					n.emit(EventWarn, map[string]any{"msg": "send_segad_err", "err": err.Error()})
				}
			} else {
				encAd := syncproto.EncodeSegAd(ad)
				if err := n.send(wire.MT_SEG_AD, encAd); err != nil {
					slog.Warn("send_segad_err", "node", n.Name, "err", err)
					n.emit(EventWarn, map[string]any{"msg": "send_segad_err", "err": err.Error()})
				}
			}

			// Send a SummaryReq with our current root; receiver will reply with SummaryResp including SV/frontier
			n.emit(EventSendSummary, map[string]any{
				"peer": n.Peer, "root": fmt.Sprintf("%x", root[:8]),
			})
			if n.ms != nil {
				if err := n.ms.Send(n.ctx, n.Peer, protoport.SummaryReqMsg{R: syncproto.SummaryReq{Root: root}}); err != nil {
					slog.Warn("send_summary_req_err", "node", n.Name, "err", err)
					n.emit(EventWarn, map[string]any{"msg": "send_summary_req_err", "err": err.Error()})
					backoff = minDur(n.backoffMax, maxDur(n.backoffBase, backoff*2))
					nextAllowed = time.Now().Add(jitter(backoff))
					return
				}
			} else {
				if err := n.send(wire.MT_SYNC_SUMMARY_REQ, syncproto.EncodeSummaryReq(syncproto.SummaryReq{Root: root})); err != nil {
					slog.Warn("send_summary_req_err", "node", n.Name, "err", err)
					n.emit(EventWarn, map[string]any{"msg": "send_summary_req_err", "err": err.Error()})
					backoff = minDur(n.backoffMax, maxDur(n.backoffBase, backoff*2))
					nextAllowed = time.Now().Add(jitter(backoff))
					return
				}
				// Cache our per-key frontier snapshot for SV-bounded receive checks
				sum := syncproto.BuildSummaryFromLog(n.Log)
				n.sessMu.Lock()
				n.sess.sentFrontier = make(map[string]uint32, len(sum.Frontier))
				for _, it := range sum.Frontier {
					n.sess.sentFrontier[it.Key] = it.From
				}
				n.sessMu.Unlock()
			}
		}
	}

	for {
		select {
		case <-n.ctx.Done():
			return

		case <-t.C:
			trySend(true)

		case <-n.kickCh:
			// debounce a burst of writes for ~50ms
			debounce := time.NewTimer(50 * time.Millisecond)
		drain:
			for {
				select {
				case <-n.kickCh:
					// keep draining while timer runs
				case <-debounce.C:
					break drain
				case <-n.ctx.Done():
					debounce.Stop()
					return
				}
			}
			trySend(false)
		}
	}
}

func genSessionID() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return uint64(time.Now().UnixNano())
	}
	return binary.BigEndian.Uint64(b[:])
}

func (n *Node) sendHB(mt byte, payload []byte) error { return n.sendHBTo(n.Peer, mt, payload) }
func (n *Node) sendHBTo(to transport.MemAddr, mt byte, payload []byte) error {
	enc := wire.Encode(mt, payload)
	if n.HB != nil {
		if !n.conn.Load() {
			return fmt.Errorf("link down")
		}
		n.emit(EventWire, map[string]any{"proto": "udp", "dir": "->", "mt": int(mt), "bytes": len(enc), "to": string(to)})
		return n.HB.Send(to, enc)
	}

	return n.send(mt, payload)
}

func (n *Node) send(mt byte, payload []byte) error { return n.sendTo(n.Peer, mt, payload) }
func (n *Node) sendTo(to transport.MemAddr, mt byte, payload []byte) error {
	if !n.conn.Load() {
		return fmt.Errorf("link down")
	}
	enc := wire.Encode(mt, payload)
	n.emit(EventWire, map[string]any{
		"proto": "tcp", "dir": "->", "mt": int(mt), "bytes": len(enc), "to": string(to),
	})
	return n.EP.Send(to, enc)
}

func (n *Node) recvLoop() {
	for {
		from, m, ok := n.ms.Recv(n.ctx)
		if !ok {
			return
		}
		switch msg := m.(type) {
		case protoport.PingMsg:
			n.emit(EventHB, map[string]any{"dir": "<-tcp"})
			if n.ms != nil {
				if err := n.ms.Send(n.ctx, from, protoport.PongMsg{}); err != nil {
					n.emit(EventWarn, map[string]any{"msg": "send_pong_err", "err": err.Error()})
				}
			} else if err := n.sendTo(from, wire.MT_PONG, nil); err != nil {
				n.emit(EventWarn, map[string]any{"msg": "send_pong_err", "err": err.Error()})
			}
		case protoport.PongMsg:
			n.emit(EventHB, map[string]any{"dir": "<-tcp"})
			n.onPong()
		case protoport.SummaryRespMsg:
			n.handleSummaryResp(from, msg.S)
		case protoport.SummaryReqMsg:
			n.handleSummaryReq(from, msg.R)
		case protoport.ReqMsg:
			n.handleReq(from, msg.R)
		case protoport.DeltaMsg:
			n.handleDelta(from, msg.D)
		case protoport.DeltaChunkMsg:
			n.handleDeltaChunk(from, msg.C)
		case protoport.AckMsg:
			n.handleAck(from, msg.A)
		case protoport.RootMsg:
			n.handleRoot(from, msg.R)
		case protoport.DescentReqMsg:
			n.handleDescentReq(from, msg.R)
		case protoport.DescentRespMsg:
			n.handleDescentResp(from, msg.R)
		case protoport.SegAdMsg:
			n.handleSegAd(from, msg.A)
		case protoport.SegKeysReqMsg:
			n.handleSegKeysReq(from, msg.R)
		case protoport.SegKeysMsg:
			n.onSegKeys(from, syncproto.EncodeSegKeys(msg.K))
		case protoport.DeltaNackMsg:
			n.onDeltaNackHandled(from, msg.N)
		default:
			slog.Warn("unknown_msg", "node", n.Name)
			n.emit(EventWarn, map[string]any{"msg": "unknown_msg"})
		}
	}
}

// (no longer needed) byte-oriented compat helpers removed after typed migration

// handleSummaryResp processes the receiver-advertised summary+SV frontier.
func (n *Node) handleSummaryResp(from transport.MemAddr, sum syncproto.Summary) {
	n.sessMu.Lock()
	n.sess.lastRemote = sum
	n.sessMu.Unlock()
	// Persist per-peer SV
	n.peerSVMu.Lock()
	n.peerSV[string(from)] = sum
	n.peerSVMu.Unlock()

	// Build needs bounded by the receiver-advertised frontier (SV)
	req := syncproto.DiffForOutbound(n.Log, sum)
	if len(req.Needs) == 0 {
		n.endSessionTo(from, true, "no_diff")
		slog.Debug("summary_noop", "node", n.Name)
		return
	}

	n.beginSessionIfNeeded()

	slog.Info("send_req", "node", n.Name, "needs", req.Needs)
	n.emit(EventSendReq, map[string]any{"needs": req.Needs})
	if n.ms != nil {
		if err := n.ms.Send(n.ctx, from, protoport.ReqMsg{R: req}); err != nil {
			slog.Warn("send_req_err", "node", n.Name, "err", err)
			n.emit(EventWarn, map[string]any{"msg": "send_req_err", "err": err.Error()})
		}
	} else if err := n.sendTo(from, wire.MT_SYNC_REQ, syncproto.EncodeReq(req)); err != nil {
		slog.Warn("send_req_err", "node", n.Name, "err", err)
		n.emit(EventWarn, map[string]any{"msg": "send_req_err", "err": err.Error()})
	}

	// Track last request to enable delta-empty repair fallback on the server side
	n.sessMu.Lock()
	n.sess.lastReq = req
	n.sess.awaitingDelta = true
	n.sess.repairAttempted = false
	n.sessMu.Unlock()
}

// handleSummaryReq replies with our summary + SV frontier for this peer.
func (n *Node) handleSummaryReq(from transport.MemAddr, req syncproto.SummaryReq) {
	// Build current summary and advertise our frontier (SV) for receiver-bounded deltas
	sum := syncproto.BuildSummaryFromLog(n.Log)
	// Log a compact SV snapshot
	n.emit(EventSendSummary, map[string]any{"peer": from, "sv_keys": len(sum.Frontier), "sv_segs": len(sum.SegFrontier)})
	if n.ms != nil {
		_ = n.ms.Send(n.ctx, from, protoport.SummaryRespMsg{S: sum})
	} else {
		_ = n.sendTo(from, wire.MT_SYNC_SUMMARY_RESP, syncproto.EncodeSummary(sum))
	}
}

func (n *Node) handleReq(from transport.MemAddr, req syncproto.Req) {
	if len(req.Needs) == 0 {
		n.endSessionTo(from, true, "no_need")
		return
	}

	n.beginSessionIfNeeded()

	// Build bounded delta according to the request (peer's From values derived from our prior SummaryResp SV)
	delta, _ := syncproto.BuildDeltaForLimited(n.Log, req, n.DeltaMaxBytes)

	// count ops
	ops := 0
	for _, e := range delta.Entries {
		ops += len(e.Ops)
	}

	if ops == 0 {
		// delta empty: if we have a recent request we sent (awaitingDelta), and
		// it asked for specific keys, force a repair attempt by re-sending From=0
		// for those keys once. This breaks loops on equal-count/different-hash.
		n.sessMu.Lock()
		lr := n.sess.lastReq
		awaiting := n.sess.awaitingDelta
		attempted := n.sess.repairAttempted
		n.sessMu.Unlock()

		if awaiting && !attempted && len(lr.Needs) > 0 {
			// mark repair attempted and send a follow-up request with From=0
			n.sessMu.Lock()
			n.sess.repairAttempted = true
			n.sessMu.Unlock()
			req0 := syncproto.Req{Needs: make([]syncproto.Need, 0, len(lr.Needs))}
			for _, need := range lr.Needs {
				req0.Needs = append(req0.Needs, syncproto.Need{Key: need.Key, From: 0})
			}
			n.emit(EventSendReq, map[string]any{"needs": req0.Needs, "reason": "delta_empty_repair"})
			if n.ms != nil {
				if err := n.ms.Send(n.ctx, from, protoport.ReqMsg{R: req0}); err != nil {
					n.emit(EventWarn, map[string]any{"msg": "send_req_err", "err": err.Error()})
				}
			} else if err := n.sendTo(from, wire.MT_SYNC_REQ, syncproto.EncodeReq(req0)); err != nil {
				n.emit(EventWarn, map[string]any{"msg": "send_req_err", "err": err.Error()})
			}
			return
		}

		slog.Info("send_delta_empty", "node", n.Name)
		_ = n.sendTo(from, wire.MT_SYNC_END, nil)
		n.endSessionTo(from, false, "delta_empty")
		return
	}
	chunks := syncproto.ChunkDelta(delta, n.DeltaMaxBytes)
	if len(chunks) == 0 {
		_ = n.sendTo(from, wire.MT_SYNC_END, nil)
		n.endSessionTo(from, false, "no_chunks")
		return
	}
	n.sessMu.Lock()
	n.sess.sndNext = 0
	// seed inflight per segment with first chunk of each segment (chunks currently may be single SID)
	n.sess.sndInflight[chunks[0].SID] = []syncproto.DeltaChunk{chunks[0]}
	ackCh := n.sess.ackCh
	sessCtx := n.sess.ctx
	n.sessMu.Unlock()

	go n.streamChunks(from, chunks, ackCh, sessCtx)
}

func (n *Node) handleDelta(from transport.MemAddr, d syncproto.Delta) {
	n.beginSessionIfNeeded()
	before := n.Log.CountPerKey()
	// Apply and merge HLC with max seen in this delta
	var maxTick uint64
	for _, e := range d.Entries {
		for _, op := range e.Ops {
			if op.HLCTicks > maxTick {
				maxTick = op.HLCTicks
			}
		}
	}
	syncproto.ApplyDelta(n.Log, d)
	if maxTick != 0 {
		_ = n.Clock.Merge(maxTick)
	}
	after := n.Log.CountPerKey()
	slog.Info("applied_delta", "node", n.Name, "keys", len(d.Entries), "counts_before", before, "counts_after", after)
	n.emit(EventAppliedDelta, map[string]any{"keys": len(d.Entries), "before": before, "after": after})

	changed := !countsEqual(before, after)

	// We received a non-empty delta; clear awaiting flag.
	n.sessMu.Lock()
	n.sess.awaitingDelta = false
	n.sessMu.Unlock()

	if n.maybeEndIfEqual(from) {
		return
	}
	// Use the last remote summary as our equality target.
	n.sessMu.Lock()
	lr := n.sess.lastRemote
	active := n.sess.active
	n.sessMu.Unlock()
	if len(lr.Leaves) == 0 || !active {
		return
	}

	if !changed {
		return
	}

	req := syncproto.DiffForReq(n.Log, lr)
	if len(req.Needs) > 0 {
		n.emit(EventSendReq, map[string]any{"needs": req.Needs, "reason": "continue"})
		if n.ms != nil {
			if err := n.ms.Send(n.ctx, from, protoport.ReqMsg{R: req}); err != nil {
				slog.Warn("send_req_err", "node", n.Name, "err", err)
				n.emit(EventWarn, map[string]any{"msg": "send_req_err", "err": err.Error()})
			}
		} else if err := n.sendTo(from, wire.MT_SYNC_REQ, syncproto.EncodeReq(req)); err != nil {
			slog.Warn("send_req_err", "node", n.Name, "err", err)
			n.emit(EventWarn, map[string]any{"msg": "send_req_err", "err": err.Error()})
		}
	}
}

func (n *Node) emit(t EventType, f map[string]any) {
	if n.Events == nil {
		return
	}
	select {
	case n.Events <- Event{Time: time.Now(), Node: n.Name, Type: t, Fields: f}:
	default:
	}
}

// heartbeatLoop moved to heartbeat.go

func (n *Node) onMiss() {
	m := n.misses.Add(1)
	if int(m) >= n.hbMissK && !n.suspect.Load() {
		n.suspect.Store(true)
		n.SetPaused(true)
		n.emit(EventHealth, map[string]any{"state": "suspect", "misses": int(m)})
	}
}

func (n *Node) onPong() {
	n.lastPong.Store(time.Now().UnixNano())
	prev := n.misses.Swap(0)
	if n.suspect.Load() {
		n.suspect.Store(false)
		if n.IsPaused() { // only unpause if we paused due to suspicion
			n.SetPaused(false)
		}
		n.emit(EventHealth, map[string]any{"state": "healthy", "misses": int(prev)})
	}
}

func (n *Node) handleSegAd(from transport.MemAddr, ad syncproto.SegAd) {
	req := syncproto.DiffForReqFromSegAd(n.Log, ad)
	if len(req.Needs) > 0 {
		n.emit(EventSendReq, map[string]any{"needs": req.Needs})
		if err := n.sendTo(from, wire.MT_SYNC_REQ, syncproto.EncodeReq(req)); err != nil {
			n.emit(EventWarn, map[string]any{"msg": "send_req_err", "err": err.Error()})
		}
		return
	}

	diff := syncproto.ChangedSegments(n.Log, ad)
	if len(diff) == 0 {
		return
	}

	sids := make([]segment.ID, 0, len(diff))
	for sid := range diff {
		sids = append(sids, sid)
	}
	slices.Sort(sids)
	if len(sids) > 64 {
		sids = sids[:64]
	}

	n.emit(EventSendSegKeysReq, map[string]any{"sids": sids})
	if n.ms != nil {
		if err := n.ms.Send(n.ctx, from, protoport.SegKeysReqMsg{R: syncproto.SegKeysReq{SIDs: sids}}); err != nil {
			n.emit(EventWarn, map[string]any{"msg": "send_segkeys_req_err", "err": err.Error()})
		}
	} else {
		if err := n.sendTo(from, wire.MT_SEG_KEYS_REQ, syncproto.EncodeSegKeysReq(syncproto.SegKeysReq{SIDs: sids})); err != nil {
			n.emit(EventWarn, map[string]any{"msg": "send_segkeys_req_err", "err": err.Error()})
		}
	}
}

func (n *Node) handleSegKeysReq(from transport.MemAddr, req syncproto.SegKeysReq) {
	view := materialize.Snapshot(n.Log)
	payload := syncproto.BuildSegKeys(view, req.SIDs)
	enc := syncproto.EncodeSegKeys(payload)
	n.emit(EventSendSegKeys, map[string]any{"items": len(payload.Items)})
	if n.ms != nil {
		if err := n.ms.Send(n.ctx, from, protoport.SegKeysMsg{K: payload}); err != nil {
			n.emit(EventWarn, map[string]any{"msg": "send_segkeys_err", "err": err.Error()})
		}
	} else if err := n.sendTo(from, wire.MT_SEG_KEYS, enc); err != nil {
		n.emit(EventWarn, map[string]any{"msg": "send_segkeys_err", "err": err.Error()})
	}
}

func (n *Node) onSegKeys(from transport.MemAddr, b []byte) {
	sk, err := syncproto.DecodeSegKeys(b)
	if err != nil {
		n.emit(EventWarn, map[string]any{"msg": "decode_segkeys_err", "err": err.Error()})
		return
	}
	counts := n.Log.CountPerKey()
	needs := make([]syncproto.Need, 0, 64)
	for _, it := range sk.Items {
		for _, p := range it.Pairs {
			fromCount := uint32(0)
			if c, ok := counts[p.Key]; ok && c > 0 {
				if c > int(^uint32(0)) {
					fromCount = ^uint32(0)
				} else {
					fromCount = uint32(c)
				}
			}
			needs = append(needs, syncproto.Need{Key: p.Key, From: fromCount})
		}
	}
	if len(needs) == 0 {
		return
	}
	req := syncproto.Req{Needs: needs}
	n.emit(EventSendReq, map[string]any{"needs": req.Needs, "reason": "seg_keys"})
	if n.ms != nil {
		if err := n.ms.Send(n.ctx, from, protoport.ReqMsg{R: req}); err != nil {
			n.emit(EventWarn, map[string]any{"msg": "send_req_err", "err": err.Error()})
		}
	} else if err := n.sendTo(from, wire.MT_SYNC_REQ, syncproto.EncodeReq(req)); err != nil {
		n.emit(EventWarn, map[string]any{"msg": "send_req_err", "err": err.Error()})
	}
}

func (n *Node) kick() {
	select {
	case n.kickCh <- struct{}{}:
	default:
	}
}

func jitter(d time.Duration) time.Duration {
	n := time.Now().UnixNano()
	frac := 0.8 + float64(n%400)/1000.0 // 0.8..1.199
	return time.Duration(float64(d) * frac)
}
func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
func maxDur(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func countsEqual(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// chunking moved to syncproto.ChunkDelta

func (n *Node) handleDeltaChunk(from transport.MemAddr, ch syncproto.DeltaChunk) {
	n.beginSessionIfNeeded()

	n.sessMu.Lock()
	expect := n.sess.rcvExpect[ch.SID]
	n.sessMu.Unlock()

	// Emit detailed receive event including SV snapshot if known
	var sv any
	n.peerSVMu.Lock()
	if s, ok := n.peerSV[string(from)]; ok {
		sv = map[string]any{"seg_frontier": len(s.SegFrontier), "keys_frontier": len(s.Frontier)}
	}
	n.peerSVMu.Unlock()
	n.emit(EventRecvDeltaChunk, map[string]any{
		"sid": int(ch.SID), "seq": ch.Seq, "last": ch.Last, "entries": len(ch.Entries), "expect": expect, "hash_ok": true, "peer_sv": sv,
	})

	// Reject anything not exactly the next expected chunk.
	if ch.Seq != expect {
		if ch.Seq < expect {
			// Duplicate/late: just ACK the last in-order; do not send a duplicate NACK.
			if expect > 0 {
				if err := n.sendTo(from, wire.MT_SYNC_ACK, syncproto.EncodeAck(syncproto.Ack{SID: ch.SID, Seq: expect - 1})); err != nil {
					n.emit(EventWarn, map[string]any{"msg": "send_ack_err", "err": err.Error(), "seq": expect - 1})
				} else {
					n.emit(EventAck, map[string]any{"dir": "send", "seq": expect - 1, "reason": "duplicate"})
				}
			}
		} else { // ch.Seq > expect
			// Out-of-window/skip: ACK last in-order and NACK the future chunk.
			if expect > 0 {
				if err := n.sendTo(from, wire.MT_SYNC_ACK, syncproto.EncodeAck(syncproto.Ack{SID: ch.SID, Seq: expect - 1})); err != nil {
					n.emit(EventWarn, map[string]any{"msg": "send_ack_err", "err": err.Error(), "seq": expect - 1})
				} else {
					n.emit(EventAck, map[string]any{"dir": "send", "seq": expect - 1, "reason": "out_of_order"})
				}
			}
			if err := n.sendTo(from, wire.MT_DELTA_NACK, syncproto.EncodeDeltaNack(syncproto.DeltaNack{Seq: ch.Seq, Code: 1})); err != nil {
				n.emit(EventWarn, map[string]any{"msg": "send_nack_err", "err": err.Error(), "seq": ch.Seq})
			} else {
				n.emit(EventAck, map[string]any{"dir": "send", "nack": true, "seq": ch.Seq, "reason": "out_of_window"})
			}
		}
		return
	}

	before := n.Log.CountPerKey()
	// SV-bounded acceptance: ensure this chunk does not start before our advertised frontier
	n.sessMu.Lock()
	sent := n.sess.sentFrontier
	n.sessMu.Unlock()
	if len(sent) > 0 {
		for _, e := range ch.Entries {
			if len(e.Ops) == 0 {
				continue
			}
			if fromCount, ok := sent[e.Key]; ok && fromCount > 0 {
				// We expect sender to start at our From (or include one predecessor for chaining)
				if before[e.Key] < int(fromCount)-1 {
					// Reject as out-of-window per SV
					if err := n.sendTo(from, wire.MT_DELTA_NACK, syncproto.EncodeDeltaNack(syncproto.DeltaNack{Seq: ch.Seq, Code: 2})); err == nil {
						n.emit(EventAck, map[string]any{"dir": "send", "nack": true, "seq": ch.Seq, "reason": "sv_out_of_window"})
					}
					return
				}
			}
		}
	}
	var maxTick uint64
	for _, e := range ch.Entries {
		for _, op := range e.Ops {
			if op.HLCTicks > maxTick {
				maxTick = op.HLCTicks
			}
			n.Log.Append(op)
		}
	}
	if maxTick != 0 {
		_ = n.Clock.Merge(maxTick)
	}
	after := n.Log.CountPerKey()
	n.emit(EventAppliedDelta, map[string]any{"keys": len(ch.Entries), "before": before, "after": after})

	// Guard: if no-op (counts unchanged) and this was the last chunk, avoid livelock.
	noChange := countsEqual(before, after)

	n.sessMu.Lock()
	n.sess.rcvExpect[ch.SID] = expect + 1
	n.sessMu.Unlock()

	if err := n.sendTo(from, wire.MT_SYNC_ACK, syncproto.EncodeAck(syncproto.Ack{SID: ch.SID, Seq: ch.Seq})); err != nil {
		n.emit(EventWarn, map[string]any{"msg": "send_ack_err", "err": err.Error(), "seq": ch.Seq})
	} else {
		n.emit(EventAck, map[string]any{"dir": "send", "seq": ch.Seq})
	}

	if ch.Last {
		// 1) Try summary-based continuation first (multiple deltas may be needed).
		if n.maybeEndIfEqual(from) {
			return
		}
		n.sessMu.Lock()
		lr := n.sess.lastRemote
		active := n.sess.active
		haveRoot := n.sess.haveRoot
		peerRoot := n.sess.lastRoot
		n.sessMu.Unlock()

		if active && len(lr.Leaves) > 0 {
			if noChange {
				// If nothing changed after applying this stream, do not immediately re-request.
				// Fall through to root/descent validation to break potential loops.
			} else {
				req := syncproto.DiffForReq(n.Log, lr)
				if len(req.Needs) > 0 {
					n.emit(EventSendReq, map[string]any{"needs": req.Needs, "reason": "continue_after_chunk"})
					if n.ms != nil {
						if err := n.ms.Send(n.ctx, from, protoport.ReqMsg{R: req}); err != nil {
							n.emit(EventWarn, map[string]any{"msg": "send_req_err", "err": err.Error()})
						}
					} else if err := n.sendTo(from, wire.MT_SYNC_REQ, syncproto.EncodeReq(req)); err != nil {
						n.emit(EventWarn, map[string]any{"msg": "send_req_err", "err": err.Error()})
					}
					// next delta stream will start at seq=0
					n.sessMu.Lock()
					n.sess.rcvExpect[ch.SID] = 0
					n.sessMu.Unlock()
					return
				}
				// If no more needs by summary, equality will be detected by the root path below.
			}
		}

		// 2) Root/descent path: if still unequal, keep descending.
		if haveRoot {
			if n.computeRootFromLog() == peerRoot {
				n.endSessionTo(from, true, "caught_up_root")
				return
			}
			// Still different: continue descent. If nothing queued, restart at root.
			n.sessMu.Lock()
			if len(n.sess.dqueue) == 0 {
				n.sess.dqueue = append(n.sess.dqueue, []byte{})
			}
			next := n.sess.dqueue[0]
			n.sess.dqueue = n.sess.dqueue[1:]
			// reset expected seq for the next delta stream
			n.sess.rcvExpect[ch.SID] = 0
			n.sessMu.Unlock()
			n.emit(EventDescent, map[string]any{"dir": "->", "kind": "req_after_delta", "prefix": string(next)})
			if n.ms != nil {
				if err := n.ms.Send(n.ctx, from, protoport.DescentReqMsg{R: syncproto.DescentReq{Prefix: next}}); err != nil {
					n.emit(EventWarn, map[string]any{"msg": "send_descent_req_err", "err": err.Error()})
				}
			} else if err := n.sendTo(from, wire.MT_DESCENT_REQ, syncproto.EncodeDescentReq(syncproto.DescentReq{Prefix: next})); err != nil {
				n.emit(EventWarn, map[string]any{"msg": "send_descent_req_err", "err": err.Error()})
			}
			return
		}
	}
}

func (n *Node) handleAck(_ transport.MemAddr, a syncproto.Ack) {

	n.emit(EventAck, map[string]any{"dir": "recv", "sid": int(a.SID), "seq": a.Seq})

	n.sessMu.Lock()
	if n.sess.ackCh != nil {
		select {
		case n.sess.ackCh <- a:
		default:
		}
	}
	// Track next seq per SID if needed later
	n.sessMu.Unlock()

}

func (n *Node) streamChunks(to transport.MemAddr, chunks []syncproto.DeltaChunk, ackCh chan syncproto.Ack, sessCtx context.Context) {
	window := n.DeltaWindowChunks
	timeout := n.RetransTimeout
	if sessCtx == nil || ackCh == nil {
		n.emit(EventWarn, map[string]any{"msg": "stream_chunks_no_session"})
		return
	}
	if window < 1 {
		window = 1
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	type inflight struct {
		ch     syncproto.DeltaChunk
		sentAt time.Time
	}
	unacked := make(map[uint32]*inflight)
	next := 0

	// Keep sess.sndInflight in sync so onDeltaNack can find any in-flight seq.
	snapshotInflight := func() {}

	// Helper to send one chunk
	sendOne := func(ch syncproto.DeltaChunk) bool {
		enc := syncproto.EncodeDeltaChunk(ch)
		n.emit(EventSendDeltaChunk, map[string]any{
			"sid": int(ch.SID), "seq": ch.Seq, "entries": len(ch.Entries), "bytes": len(enc), "last": ch.Last,
		})
		var err error
		if n.ms != nil {
			err = n.ms.Send(n.ctx, to, protoport.DeltaChunkMsg{C: ch})
		} else {
			err = n.sendTo(to, wire.MT_SYNC_DELTA_CHUNK, enc)
		}
		if err != nil {
			n.emit(EventWarn, map[string]any{"msg": "send_delta_chunk_err", "err": err.Error()})
			return false
		}
		unacked[ch.Seq] = &inflight{ch: ch, sentAt: time.Now()}
		snapshotInflight()
		return true
	}

	// Initial fill
	for next < len(chunks) && len(unacked) < window {
		if !sendOne(chunks[next]) {
			return
		}
		next++
	}

	tick := time.NewTicker(timeout / 2)
	defer tick.Stop()

	for {
		if len(unacked) == 0 && next >= len(chunks) {
			return
		}

		select {
		case <-sessCtx.Done():
			return
		case <-n.ctx.Done():
			return

		case ack := <-ackCh:
			// Ack always increases by 1 (receiver is in-order)
			_ = ack.SID
			delete(unacked, ack.Seq)
			snapshotInflight()
			// Top up window
			for next < len(chunks) && len(unacked) < window {
				if !sendOne(chunks[next]) {
					return
				}
				next++
			}

		case <-tick.C:
			// Retransmit any expired chunk
			now := time.Now()
			for s, inf := range unacked {
				if now.Sub(inf.sentAt) >= timeout {
					// resend
					enc := syncproto.EncodeDeltaChunk(inf.ch)
					n.emit(EventSendDeltaChunk, map[string]any{
						"sid": int(inf.ch.SID), "seq": inf.ch.Seq, "entries": len(inf.ch.Entries), "bytes": len(enc), "last": inf.ch.Last, "retrans": true,
					})
					var err error
					if n.ms != nil {
						err = n.ms.Send(n.ctx, to, protoport.DeltaChunkMsg{C: inf.ch})
					} else {
						err = n.sendTo(to, wire.MT_SYNC_DELTA_CHUNK, enc)
					}
					if err != nil {
						n.emit(EventWarn, map[string]any{"msg": "send_delta_chunk_err", "err": err.Error()})
						return
					}
					inf.sentAt = now
					// keep inacked
					_ = s
				}
			}
			snapshotInflight()
		}
	}
}

func (n *Node) handleRoot(from transport.MemAddr, r syncproto.Root) {

	my := n.computeRootFromLog()
	if my == r.Hash {
		n.endSessionTo(from, true, "root_equal")
		return
	}
	if !n.DescentEnabled {
		return
	}

	n.beginSessionIfNeeded()

	n.sessMu.Lock()
	n.sess.lastRoot = r.Hash
	n.sess.haveRoot = true
	n.sessMu.Unlock()

	n.emit(EventDescent, map[string]any{"dir": "->", "kind": "req", "prefix": ""})
	req := syncproto.DescentReq{Prefix: nil}
	if err := n.sendTo(from, wire.MT_DESCENT_REQ, syncproto.EncodeDescentReq(req)); err != nil {
		n.emit(EventWarn, map[string]any{"msg": "send_descent_req_err", "err": err.Error()})
	}
}

func (n *Node) handleDescentReq(from transport.MemAddr, req syncproto.DescentReq) {
	if !n.DescentEnabled {
		return
	}
	view := materialize.Snapshot(n.Log)
	resp := syncproto.BuildDescentResp(view, req.Prefix, n.DescentLeafK)
	kind := "resp_children"
	if len(resp.Leaves) > 0 {
		kind = "resp_leaves"
	}
	n.emit(EventDescent, map[string]any{"dir": "<-", "kind": kind, "prefix": string(req.Prefix)})
	if err := n.sendTo(from, wire.MT_DESCENT_RESP, syncproto.EncodeDescentResp(resp)); err != nil {
		n.emit(EventWarn, map[string]any{"msg": "send_descent_resp_err", "err": err.Error()})
	}
}

func (n *Node) handleDescentResp(from transport.MemAddr, resp syncproto.DescentResp) {
	if !n.DescentEnabled {
		return
	}

	all := n.sortedLeaves()
	counts := n.Log.CountPerKey()
	needs, nextPrefixes := syncproto.DiffDescent(all, resp, counts)
	if len(needs) > 0 {
		req := syncproto.Req{Needs: needs}
		n.emit(EventSendReq, map[string]any{"needs": req.Needs, "reason": "descent"})
		if err := n.sendTo(from, wire.MT_SYNC_REQ, syncproto.EncodeReq(req)); err != nil {
			n.emit(EventWarn, map[string]any{"msg": "send_req_err", "err": err.Error()})
		}
	}
	if len(nextPrefixes) > 0 {
		n.sessMu.Lock()
		n.sess.dqueue = append(n.sess.dqueue, nextPrefixes...)
		next := n.sess.dqueue[0]
		n.sess.dqueue = n.sess.dqueue[1:]
		n.sessMu.Unlock()
		n.emit(EventDescent, map[string]any{"dir": "->", "kind": "req_next", "prefix": string(next)})
		if err := n.sendTo(from, wire.MT_DESCENT_REQ, syncproto.EncodeDescentReq(syncproto.DescentReq{Prefix: next})); err != nil {
			n.emit(EventWarn, map[string]any{"msg": "send_descent_req_err", "err": err.Error()})
		}
		return
	}
}

// sortedLeaves returns a sorted snapshot of leaves
func (n *Node) sortedLeaves() []merkle.Leaf {
	snap := materialize.Snapshot(n.Log)
	ls := materialize.LeavesFromSnapshot(snap)
	sortLeaves(ls)
	return ls
}

func (n *Node) warn(msg string, err error) {
	if err != nil {
		n.emit(EventWarn, map[string]any{"msg": msg, "err": err.Error()})
	}
}

func (n *Node) beginSessionIfNeeded() {
	n.sessMu.Lock()
	defer n.sessMu.Unlock()
	if n.sess.active {
		return
	}
	n.sess.id = genSessionID()
	n.sess.active = true
	n.sess.sndNext = 0
	n.sess.sndInflight = make(map[segment.ID][]syncproto.DeltaChunk)
	n.sess.rcvExpect = make(map[segment.ID]uint32)
	bufCap := 32
	if w := 4 * n.DeltaWindowChunks; w > bufCap {
		bufCap = w
	}
	n.sess.ackCh = make(chan syncproto.Ack, bufCap)
	n.sess.ctx, n.sess.cancel = context.WithCancel(n.ctx)
	n.emit(EventSync, map[string]any{"action": "begin", "id": n.sess.id})
}

func (n *Node) endSessionTo(to transport.MemAddr, sendEnd bool, reason string) {
	n.sessMu.Lock()
	active := n.sess.active
	id := n.sess.id
	cancel := n.sess.cancel
	// Clear session state regardless so we don't leave stale inflight.
	n.sess = syncSession{}
	n.sessMu.Unlock()
	// Send END even if this side didn't mark the session active.
	if sendEnd {
		if n.ms == nil {
			_ = n.sendTo(to, wire.MT_SYNC_END, nil)
		}
	}
	if active && cancel != nil {
		cancel()
	}
	if active {
		n.emit(EventSync, map[string]any{"action": "end", "id": id, "reason": reason})
	}
}

func sortLeaves(ls []merkle.Leaf) {
	slices.SortFunc(ls, func(a, b merkle.Leaf) int {
		switch {
		case a.Key < b.Key:
			return -1
		case a.Key > b.Key:
			return 1
		default:
			return 0
		}
	})
}
func (n *Node) computeRootFromLog() [32]byte {
	view := materialize.Snapshot(n.Log)
	ls := materialize.LeavesFromSnapshot(view)
	sortLeaves(ls)
	return merkle.Build(ls)
}

func (n *Node) maybeEndIfEqual(to transport.MemAddr) bool {
	// use lastRemote if available
	n.sessMu.Lock()
	lr := n.sess.lastRemote
	active := n.sess.active
	n.sessMu.Unlock()
	if !active || len(lr.Leaves) == 0 {
		return false
	}

	my := materialize.Snapshot(n.Log)
	myLeaves := materialize.LeavesFromSnapshot(my)
	if len(lr.Leaves) != len(myLeaves) {
		return false
	}
	remote := make(map[string][32]byte, len(lr.Leaves))
	for _, lf := range lr.Leaves {
		remote[lf.Key] = lf.Hash
	}
	for _, lf := range myLeaves {
		if remote[lf.Key] != lf.Hash {
			return false
		}
	}
	n.endSessionTo(to, true, "caught_up")
	return true
}

func (n *Node) onDeltaNackHandled(from transport.MemAddr, nack syncproto.DeltaNack) {
	n.emit(EventAck, map[string]any{"dir": "recv", "nack": true, "seq": nack.Seq, "code": nack.Code})

	// We keep the last sent chunk in sess.sndInflight (window is small).
	n.sessMu.Lock()
	infl := n.sess.sndInflight
	n.sessMu.Unlock()
	if len(infl) == 0 {
		n.emit(EventWarn, map[string]any{"msg": "nack_no_inflight"})
		return
	}
	if inflist, ok := infl[0]; ok {
		for _, ch := range inflist {
			if ch.Seq != nack.Seq {
				continue
			}
			enc := syncproto.EncodeDeltaChunk(ch)
			n.emit(EventSendDeltaChunk, map[string]any{
				"sid": int(ch.SID), "seq": ch.Seq, "entries": len(ch.Entries), "bytes": len(enc), "last": ch.Last, "retrans": true, "reason": "nack",
			})
			if err := n.sendTo(from, wire.MT_SYNC_DELTA_CHUNK, enc); err != nil {
				n.emit(EventWarn, map[string]any{"msg": "retransmit_err", "seq": ch.Seq, "err": err.Error()})
			}
			return
		}
	}
	// No match in inflight (could be stale); log and move on.
	n.emit(EventWarn, map[string]any{"msg": "nack_unknown_seq", "seq": nack.Seq})
}
