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
	"github.com/juanpablocruz/maep/pkg/syncproto"
	"github.com/juanpablocruz/maep/pkg/transport"
	"github.com/juanpablocruz/maep/pkg/wire"
)

type syncSession struct {
	id         uint64
	active     bool
	lastRemote syncproto.Summary
}

type Node struct {
	Name   string
	Peer   transport.MemAddr
	EP     *transport.Endpoint
	Log    *oplog.Log
	Clock  *hlc.Clock
	Ticker time.Duration

	Events chan Event

	DeltaMaxBytes int

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
}

func New(name string, ep *transport.Endpoint, peer transport.MemAddr, tickEvery time.Duration) *Node {
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		Name:          name,
		Peer:          peer,
		EP:            ep,
		Log:           oplog.New(),
		Clock:         hlc.New(),
		Ticker:        tickEvery,
		DeltaMaxBytes: 32 * 1024,
		kickCh:        make(chan struct{}, 1),
		backoffBase:   500 * time.Millisecond,
		backoffMax:    5 * time.Second,
		hbEvery:       350 * time.Millisecond,
		hbMissK:       6,
		ctx:           ctx, cancel: cancel,
	}

	n.conn.Store(true)
	n.paused.Store(false)
	return n
}

func (n *Node) AttachEvents(ch chan Event) { n.Events = ch }

func (n *Node) Start() {
	go n.recvLoop()
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
func (n *Node) IsPaused() bool { return n.paused.Load() }

func (n *Node) Put(key string, val []byte, actor model.ActorID) {
	op := model.Op{
		Version:  model.OpSchemaV1,
		Kind:     model.OpKindPut,
		Key:      key,
		Value:    slices.Clone(val),
		HLCTicks: n.Clock.Now(),
		Actor:    actor,
	}
	op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor)
	n.Log.Append(op)
	slog.Info("put", "node", n.Name, "key", key, "val", string(val), "hlc", op.HLCTicks)
	n.emit(EventPut, map[string]any{"key": key, "val": string(val), "hlc": op.HLCTicks})
	n.kick()
}

func (n *Node) Delete(key string, actor model.ActorID) {
	op := model.Op{
		Version:  model.OpSchemaV1,
		Kind:     model.OpKindDel,
		Key:      key,
		HLCTicks: n.Clock.Now(),
		Actor:    actor,
	}
	op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor)
	n.Log.Append(op)
	slog.Info("del", "node", n.Name, "key", key, "hlc", op.HLCTicks)
	n.emit(EventDel, map[string]any{"key": key, "hlc": op.HLCTicks})
	n.kick()
}

func (n *Node) summaryLoop() {
	t := time.NewTicker(n.Ticker)
	defer t.Stop()

	var lastRoot [32]byte
	backoff := n.backoffBase
	nextAllowed := time.Now() // earliest time we may try to send

	trySend := func() {
		// guard: paused or link down
		if n.paused.Load() || !n.conn.Load() || time.Now().Before(nextAllowed) {
			return
		}

		sum := syncproto.BuildSummaryFromLog(n.Log)
		// compute root
		ls := make([]merkle.Leaf, 0, len(sum.Leaves))
		for _, lf := range sum.Leaves {
			ls = append(ls, merkle.Leaf{Key: lf.Key, Hash: lf.Hash})
		}
		root := merkle.Build(ls)

		// only send if root changed
		if bytes.Equal(root[:], lastRoot[:]) {
			return
		}

		n.beginSession()

		// (A) best-effort SEG_AD (doesn't affect backoff timer)
		ad := syncproto.BuildSegAdFromLog(n.Log)
		encAd := syncproto.EncodeSegAd(ad)
		n.emit(EventSendSegAd, map[string]any{"items": len(ad.Items)})
		if err := n.send(wire.MT_SEG_AD, encAd); err != nil {
			slog.Warn("send_segad_err", "node", n.Name, "err", err)
			n.emit(EventWarn, map[string]any{"msg": "send_segad_err", "err": err.Error()})
			// keep going; SUMMARY is the driver
		}

		// (B) full SUMMARY drives REQ/DELTA
		slog.Info("send_summary", "node", n.Name, "peer", n.Peer, "leaves", len(sum.Leaves))
		n.emit(EventSendSummary, map[string]any{
			"peer": n.Peer, "leaves": len(sum.Leaves), "root": fmt.Sprintf("%x", root[:8]),
		})
		if err := n.send(wire.MT_SYNC_SUMMARY, syncproto.EncodeSummary(sum)); err != nil {
			// backoff with jitter on failure
			slog.Warn("send_summary_err", "node", n.Name, "err", err)
			n.emit(EventWarn, map[string]any{"msg": "send_summary_err", "err": err.Error()})
			backoff = minDur(n.backoffMax, maxDur(n.backoffBase, backoff*2))
			nextAllowed = time.Now().Add(jitter(backoff))
			return
		}

		// success → reset backoff and advance root
		backoff = n.backoffBase
		nextAllowed = time.Now() // can send again any time root changes
		lastRoot = root
	}

	for {
		select {
		case <-n.ctx.Done():
			return

		case <-t.C:
			trySend()

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
			trySend()
		}
	}
}

func (n *Node) beginSession() {
	n.sessMu.Lock()
	defer n.sessMu.Unlock()
	if n.sess.active {
		return
	}
	n.sess.id = genSessionID()
	n.sess.active = true
	n.emit(EventSync, map[string]any{"action": "begin", "id": n.sess.id})
	_ = n.send(wire.MT_SYNC_BEGIN, nil)
}

func (n *Node) endSession(sendEnd bool, reason string) {
	n.sessMu.Lock()
	defer n.sessMu.Unlock()
	if !n.sess.active {
		return
	}
	if sendEnd {
		_ = n.send(wire.MT_SYNC_END, nil)
	}
	n.emit(EventSync, map[string]any{"action": "end", "id": n.sess.id, "reason": reason})
	n.sess = syncSession{}
}

func genSessionID() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return uint64(time.Now().UnixNano())
	}
	return binary.BigEndian.Uint64(b[:])
}

func (n *Node) send(mt byte, payload []byte) error {
	if !n.conn.Load() {
		return fmt.Errorf("link down")
	}
	return n.EP.Send(n.Peer, wire.Encode(mt, payload))
}

func (n *Node) recvLoop() {
	for {
		frame, ok := n.EP.Recv(n.ctx)
		if !ok {
			return
		}
		mt, payload, err := wire.Decode(frame)
		if err != nil {
			slog.Warn("wire_decode_err", "node", n.Name, "err", err)
			n.emit(EventWarn, map[string]any{"msg": "wire_decode_err", "err": err.Error()})
			continue
		}
		switch mt {
		case wire.MT_SYNC_SUMMARY:
			n.onSummary(payload)
		case wire.MT_SYNC_REQ:
			n.onReq(payload)
		case wire.MT_SYNC_DELTA:
			n.onDelta(payload)
		case wire.MT_PING:
			n.emit(EventHB, map[string]any{"dir": "<-"})
			if err := n.send(wire.MT_PONG, nil); err != nil {
				n.emit(EventWarn, map[string]any{"msg": "send_pong_err", "err": err.Error()})
			}
		case wire.MT_PONG:
			n.emit(EventHB, map[string]any{"dir": "<-"})
			n.onPong()
		case wire.MT_SYNC_BEGIN:
			n.sessMu.Lock()
			if !n.sess.active {
				n.sess.id = genSessionID()
				n.sess.active = true
				n.emit(EventSync, map[string]any{"action": "begin", "id": n.sess.id})
			}
			n.sessMu.Unlock()

		case wire.MT_SYNC_END:
			n.endSession(false, "peer_end")
		case wire.MT_SEG_AD:
			n.onSegAd(payload)
		default:
			slog.Warn("unknown_msg", "node", n.Name, "mt", mt)
			n.emit(EventWarn, map[string]any{"msg": "unknown_msg", "mt": mt})
		}
	}
}

func (n *Node) onSummary(b []byte) {
	sum, err := syncproto.DecodeSummary(b)
	if err != nil {
		slog.Warn("decode_summary_err", "node", n.Name, "err", err)
		n.emit(EventWarn, map[string]any{"msg": "decode_summary_err", "err": err.Error()})
		return
	}
	// remember remote summary
	n.sessMu.Lock()
	n.sess.lastRemote = sum
	n.sessMu.Unlock()

	req := syncproto.DiffForReq(n.Log, sum)
	if len(req.Needs) == 0 {
		n.endSession(true, "no_diff")
		slog.Debug("summary_noop", "node", n.Name)
		return
	}

	slog.Info("send_req", "node", n.Name, "needs", req.Needs)
	n.emit(EventSendReq, map[string]any{"needs": req.Needs})
	if err := n.send(wire.MT_SYNC_REQ, syncproto.EncodeReq(req)); err != nil {
		slog.Warn("send_req_err", "node", n.Name, "err", err)
		n.emit(EventWarn, map[string]any{"msg": "send_req_err", "err": err.Error()})
	}
}

func (n *Node) onReq(b []byte) {
	req, err := syncproto.DecodeReq(b)
	if err != nil {
		slog.Warn("decode_req_err", "node", n.Name, "err", err)
		n.emit(EventWarn, map[string]any{"msg": "decode_req_err", "err": err.Error()})
		return
	}
	if len(req.Needs) == 0 {
		n.endSession(true, "no_need")
		return
	}

	delta, truncated := syncproto.BuildDeltaForLimited(n.Log, req, n.DeltaMaxBytes)

	ops := 0
	for _, e := range delta.Entries {
		ops += len(e.Ops)
	}
	enc := syncproto.EncodeDelta(delta)

	slog.Info("send_delta", "node", n.Name, "entries", len(delta.Entries), "ops", ops, "bytes", len(enc), "partial", truncated)
	n.emit(EventSendDelta, map[string]any{"entries": len(delta.Entries), "ops": ops, "bytes": len(enc), "partial": truncated})

	if err := n.send(wire.MT_SYNC_DELTA, enc); err != nil {
		slog.Warn("send_delta_err", "node", n.Name, "err", err)
		n.emit(EventWarn, map[string]any{"msg": "send_delta_err", "err": err.Error()})
	}
}

func (n *Node) onDelta(b []byte) {
	d, err := syncproto.DecodeDelta(b)
	if err != nil {
		slog.Warn("decode_delta_err", "node", n.Name, "err", err)
		n.emit(EventWarn, map[string]any{"msg": "decode_delta_err", "err": err.Error()})
		return
	}
	before := n.Log.CountPerKey()
	syncproto.ApplyDelta(n.Log, d)
	after := n.Log.CountPerKey()
	slog.Info("applied_delta", "node", n.Name, "keys", len(d.Entries), "counts_before", before, "counts_after", after)
	n.emit(EventAppliedDelta, map[string]any{"keys": len(d.Entries), "before": before, "after": after})

	// Use the last remote summary as our equality target.
	n.sessMu.Lock()
	lr := n.sess.lastRemote
	active := n.sess.active
	n.sessMu.Unlock()
	if len(lr.Leaves) == 0 || !active {
		return
	}

	// Compare our current visible state to the remote summary.
	myView := materialize.Snapshot(n.Log)
	myLeaves := materialize.LeavesFromSnapshot(myView)

	if len(lr.Leaves) == len(myLeaves) {
		remote := make(map[string][32]byte, len(lr.Leaves))
		for _, lf := range lr.Leaves {
			remote[lf.Key] = lf.Hash
		}
		equal := true
		for _, lf := range myLeaves {
			if remote[lf.Key] != lf.Hash {
				equal = false
				break
			}
		}
		if equal {
			n.endSession(true, "caught_up")
			return
		}
	}

	// Not equal yet → send a follow-up REQ to fetch the remaining ops.
	req := syncproto.DiffForReq(n.Log, lr)
	if len(req.Needs) > 0 {
		n.emit(EventSendReq, map[string]any{"needs": req.Needs, "reason": "continue"})
		if err := n.send(wire.MT_SYNC_REQ, syncproto.EncodeReq(req)); err != nil {
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
	default: // drop if UI is slow
	}
}

func (n *Node) heartbeatLoop() {
	t := time.NewTicker(n.hbEvery)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
			if !n.conn.Load() {
				n.onMiss()
				continue
			}

			if err := n.send(wire.MT_PING, nil); err != nil {
				n.emit(EventHB, map[string]any{"dir": "->", "err": err.Error()})
				n.onMiss()
				continue
			}
			n.emit(EventHB, map[string]any{"dir": "->"})
			n.onMiss()
		}
	}
}

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

func (n *Node) onSegAd(b []byte) {
	ad, err := syncproto.DecodeSegAd(b)
	if err != nil {
		n.emit(EventWarn, map[string]any{"msg": "decode_segad_err", "err": err.Error()})
		return
	}
	req := syncproto.DiffForReqFromSegAd(n.Log, ad)
	if len(req.Needs) == 0 {
		return
	}
	n.emit(EventSendReq, map[string]any{"needs": req.Needs})
	if err := n.send(wire.MT_SYNC_REQ, syncproto.EncodeReq(req)); err != nil {
		n.emit(EventWarn, map[string]any{"msg": "send_req_err", "err": err.Error()})
	}
}

func (n *Node) kick() {
	select {
	case n.kickCh <- struct{}{}:
	default:
	} // collapse bursts
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
