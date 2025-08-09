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
	sndInflight []syncproto.DeltaChunk
	rcvExpect   uint32
	ackCh       chan uint32
	ctx         context.Context
	cancel      context.CancelFunc
	dqueue      [][]byte
	lastRoot    [32]byte
	haveRoot    bool
}

type Node struct {
	Name   string
	Peer   transport.MemAddr
	EP     transport.EndpointIF
	HB     transport.EndpointIF
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
	}

	n.conn.Store(true)
	n.paused.Store(false)
	return n
}

func (n *Node) AttachHB(ep transport.EndpointIF) { n.HB = ep }

func (n *Node) AttachEvents(ch chan Event) { n.Events = ch }

func (n *Node) Start() {
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

		if err := n.send(wire.MT_SYNC_ROOT, syncproto.EncodeRoot(syncproto.Root{Hash: root})); err != nil {
			n.warn("send_root_err", err)
			backoff = minDur(n.backoffMax, maxDur(n.backoffBase, backoff*2))
			nextAllowed = time.Now().Add(jitter(backoff))
			return
		}
		backoff = n.backoffBase
		nextAllowed = time.Now()
		lastRoot = root
		lastSend = time.Now()

		if !n.DescentEnabled {
			sum := syncproto.BuildSummaryFromLog(n.Log)
			ad := syncproto.BuildSegAdFromLog(n.Log)
			encAd := syncproto.EncodeSegAd(ad)
			n.emit(EventSendSegAd, map[string]any{"items": len(ad.Items)})
			if err := n.send(wire.MT_SEG_AD, encAd); err != nil {
				slog.Warn("send_segad_err", "node", n.Name, "err", err)
				n.emit(EventWarn, map[string]any{"msg": "send_segad_err", "err": err.Error()})
			}

			slog.Info("send_summary", "node", n.Name, "peer", n.Peer, "leaves", len(sum.Leaves))
			n.emit(EventSendSummary, map[string]any{
				"peer": n.Peer, "leaves": len(sum.Leaves), "root": fmt.Sprintf("%x", root[:8]),
			})
			if err := n.send(wire.MT_SYNC_SUMMARY, syncproto.EncodeSummary(sum)); err != nil {
				slog.Warn("send_summary_err", "node", n.Name, "err", err)
				n.emit(EventWarn, map[string]any{"msg": "send_summary_err", "err": err.Error()})
				backoff = minDur(n.backoffMax, maxDur(n.backoffBase, backoff*2))
				nextAllowed = time.Now().Add(jitter(backoff))
				return
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
		var from transport.MemAddr
		var frame []byte
		var ok bool

		if ep, ok2 := n.EP.(transport.FromEndpoint); ok2 {
			from, frame, ok = ep.RecvFrom(n.ctx)
		} else {
			frame, ok = n.EP.Recv(n.ctx)
			from = n.Peer
		}
		if !ok {
			return
		}
		mt, payload, err := wire.Decode(frame)
		if err != nil {
			slog.Warn("wire_decode_err", "node", n.Name, "err", err)
			n.emit(EventWarn, map[string]any{"msg": "wire_decode_err", "err": err.Error()})
			continue
		}
		n.emit(EventWire, map[string]any{"proto": "tcp", "dir": "<-", "mt": int(mt), "bytes": len(frame)})
		switch mt {
		case wire.MT_SYNC_SUMMARY:
			n.onSummary(from, payload)
		case wire.MT_SYNC_REQ:
			n.onReq(from, payload)
		case wire.MT_SYNC_DELTA:
			n.onDelta(from, payload)
		case wire.MT_PING:
			n.emit(EventHB, map[string]any{"dir": "<-tcp"})
			if err := n.sendTo(from, wire.MT_PONG, nil); err != nil {
				n.emit(EventWarn, map[string]any{"msg": "send_pong_err", "err": err.Error()})
			}
		case wire.MT_PONG:
			n.emit(EventHB, map[string]any{"dir": "<-tcp"})
			n.onPong()
		case wire.MT_SYNC_BEGIN:
			n.beginSessionIfNeeded()
		case wire.MT_SYNC_END:
			n.endSessionTo(from, false, "peer_end")
		case wire.MT_SEG_AD:
			n.onSegAd(from, payload)
		case wire.MT_SEG_KEYS_REQ:
			n.onSegKeysReq(from, payload)
		case wire.MT_SEG_KEYS:
			n.onSegKeys(from, payload)
		case wire.MT_SYNC_DELTA_CHUNK:
			n.onDeltaChunk(from, payload)
		case wire.MT_SYNC_ACK:
			n.onAck(from, payload)
		case wire.MT_SYNC_ROOT:
			n.onRoot(from, payload)
		case wire.MT_DESCENT_REQ:
			n.onDescentReq(from, payload)
		case wire.MT_DESCENT_RESP:
			n.onDescentResp(from, payload)
		case wire.MT_DELTA_NACK:
			n.onDeltaNack(from, payload)
		default:
			slog.Warn("unknown_msg", "node", n.Name, "mt", mt)
			n.emit(EventWarn, map[string]any{"msg": "unknown_msg", "mt": mt})
		}
	}
}

func (n *Node) onSummary(from transport.MemAddr, b []byte) {
	sum, err := syncproto.DecodeSummary(b)
	if err != nil {
		slog.Warn("decode_summary_err", "node", n.Name, "err", err)
		n.emit(EventWarn, map[string]any{"msg": "decode_summary_err", "err": err.Error()})
		return
	}
	n.sessMu.Lock()
	n.sess.lastRemote = sum
	n.sessMu.Unlock()

	req := syncproto.DiffForReq(n.Log, sum)
	if len(req.Needs) == 0 {
		n.endSessionTo(from, true, "no_diff")
		slog.Debug("summary_noop", "node", n.Name)
		return
	}

	n.beginSessionIfNeeded()

	slog.Info("send_req", "node", n.Name, "needs", req.Needs)
	n.emit(EventSendReq, map[string]any{"needs": req.Needs})
	if err := n.sendTo(from, wire.MT_SYNC_REQ, syncproto.EncodeReq(req)); err != nil {
		slog.Warn("send_req_err", "node", n.Name, "err", err)
		n.emit(EventWarn, map[string]any{"msg": "send_req_err", "err": err.Error()})
	}
}

func (n *Node) onReq(from transport.MemAddr, b []byte) {
	req, err := syncproto.DecodeReq(b)
	if err != nil {
		return
	}
	if len(req.Needs) == 0 {
		n.endSessionTo(from, true, "no_need")
		return
	}

	n.beginSessionIfNeeded()

	delta, _ := syncproto.BuildDeltaForLimited(n.Log, req, n.DeltaMaxBytes)

	// count ops
	ops := 0
	for _, e := range delta.Entries {
		ops += len(e.Ops)
	}

	if ops == 0 {
		slog.Info("send_delta_empty", "node", n.Name)
		_ = n.sendTo(from, wire.MT_SYNC_END, nil)
		return
	}
	chunks := n.chunkDelta(delta, n.DeltaMaxBytes)
	if len(chunks) == 0 {
		_ = n.sendTo(from, wire.MT_SYNC_END, nil)
		return
	}
	n.sessMu.Lock()
	n.sess.sndNext = 0
	n.sess.sndInflight = []syncproto.DeltaChunk{chunks[0]}
	ackCh := n.sess.ackCh
	sessCtx := n.sess.ctx
	n.sessMu.Unlock()

	go n.streamChunks(from, chunks, ackCh, sessCtx)
}

func (n *Node) onDelta(from transport.MemAddr, b []byte) {
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

	changed := !countsEqual(before, after)

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
		if err := n.sendTo(from, wire.MT_SYNC_REQ, syncproto.EncodeReq(req)); err != nil {
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

			if err := n.sendHB(wire.MT_PING, nil); err != nil {
				dir := "->udp"
				if n.HB == nil {
					dir = "->tcp"
				}
				n.emit(EventHB, map[string]any{"dir": dir, "err": err.Error()})
				n.onMiss()
				continue
			}
			{
				dir := "->udp"
				if n.HB == nil {
					dir = "->tcp"
				}
				n.emit(EventHB, map[string]any{"dir": dir})
			}
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

func (n *Node) onSegAd(from transport.MemAddr, b []byte) {
	ad, err := syncproto.DecodeSegAd(b)
	if err != nil {
		n.emit(EventWarn, map[string]any{"msg": "decode_segad_err", "err": err.Error()})
		return
	}
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
	if err := n.sendTo(from, wire.MT_SEG_KEYS_REQ, syncproto.EncodeSegKeysReq(syncproto.SegKeysReq{SIDs: sids})); err != nil {
		n.emit(EventWarn, map[string]any{"msg": "send_segkeys_req_err", "err": err.Error()})
	}
}

func (n *Node) onSegKeysReq(from transport.MemAddr, b []byte) {
	req, err := syncproto.DecodeSegKeysReq(b)
	if err != nil {
		n.emit(EventWarn, map[string]any{"msg": "decode_segkeys_req_err", "err": err.Error()})
		return
	}
	view := materialize.Snapshot(n.Log)
	payload := syncproto.BuildSegKeys(view, req.SIDs)
	enc := syncproto.EncodeSegKeys(payload)
	n.emit(EventSendSegKeys, map[string]any{"items": len(payload.Items)})
	if err := n.sendTo(from, wire.MT_SEG_KEYS, enc); err != nil {
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
	if err := n.sendTo(from, wire.MT_SYNC_REQ, syncproto.EncodeReq(req)); err != nil {
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

func (n *Node) chunkDelta(d syncproto.Delta, maxBytes int) []syncproto.DeltaChunk {
	if maxBytes <= 0 {
		maxBytes = n.DeltaMaxBytes
	}
	seq := uint32(0)
	var out []syncproto.DeltaChunk
	cur := syncproto.DeltaChunk{Seq: seq, Last: false, Entries: nil}
	room := maxBytes

	estEntrySize := func(e syncproto.DeltaEntry) int {
		// rough upper bound: key + per-op fixed parts + values
		s := 2 + len(e.Key) + 4
		for _, op := range e.Ops {
			s += 2 + 1 + 8 + 8 + 16 + 32 + 4 + len(op.Value)
		}
		return s
	}
	flush := func() {
		if len(cur.Entries) == 0 {
			return
		}
		out = append(out, cur)
		seq++
		cur = syncproto.DeltaChunk{Seq: seq}
		room = maxBytes
	}
	for _, e := range d.Entries {
		sz := estEntrySize(e)
		if sz > maxBytes {
			flush()
			out = append(out, syncproto.DeltaChunk{Seq: seq, Entries: []syncproto.DeltaEntry{e}})
			seq++
			cur = syncproto.DeltaChunk{Seq: seq}
			room = maxBytes
			continue
		}
		if sz > room {
			flush()
		}
		cur.Entries = append(cur.Entries, e)
		room -= sz
	}
	flush()
	if len(out) > 0 {
		out[len(out)-1].Last = true
	}
	return out
}

func (n *Node) onDeltaChunk(from transport.MemAddr, b []byte) {
	ch, err := syncproto.DecodeDeltaChunk(b)
	if err != nil {
		// If decoder populated Seq (bad hash case), NACK it.
		if ch.Seq != 0 {
			_ = n.sendTo(from, wire.MT_DELTA_NACK, syncproto.EncodeDeltaNack(syncproto.DeltaNack{
				Seq:  ch.Seq,
				Code: 0, // 0 = bad_hash
			}))
			n.emit(EventAck, map[string]any{"dir": "send", "nack": true, "seq": ch.Seq, "reason": "bad_hash"})
		}
		n.emit(EventWarn, map[string]any{"msg": "decode_delta_chunk_err", "err": err.Error()})
		return
	}

	n.sessMu.Lock()
	expect := n.sess.rcvExpect
	n.sessMu.Unlock()

	n.emit(EventRecvDeltaChunk, map[string]any{
		"seq": ch.Seq, "last": ch.Last, "entries": len(ch.Entries), "expect": expect, "hash_ok": true,
	})

	// Reject anything not exactly the next expected chunk.
	if ch.Seq != expect {
		if ch.Seq < expect {
			// Duplicate/late: ACK previous-in-order to hint resync; also NACK duplicate.
			if expect > 0 {
				_ = n.sendTo(from, wire.MT_SYNC_ACK, syncproto.EncodeAck(syncproto.Ack{Seq: expect - 1}))
				n.emit(EventAck, map[string]any{"dir": "send", "seq": expect - 1, "reason": "duplicate"})
			}
			_ = n.sendTo(from, wire.MT_DELTA_NACK, syncproto.EncodeDeltaNack(syncproto.DeltaNack{
				Seq:  ch.Seq,
				Code: 2, // 2 = duplicate
			}))
			n.emit(EventAck, map[string]any{"dir": "send", "nack": true, "seq": ch.Seq, "reason": "duplicate"})
		} else { // ch.Seq > expect
			// Out-of-window/skip: ACK last in-order and NACK the future chunk.
			if expect > 0 {
				_ = n.sendTo(from, wire.MT_SYNC_ACK, syncproto.EncodeAck(syncproto.Ack{Seq: expect - 1}))
				n.emit(EventAck, map[string]any{"dir": "send", "seq": expect - 1, "reason": "out_of_order"})
			}
			_ = n.sendTo(from, wire.MT_DELTA_NACK, syncproto.EncodeDeltaNack(syncproto.DeltaNack{
				Seq:  ch.Seq,
				Code: 1, // 1 = out_of_window
			}))
			n.emit(EventAck, map[string]any{"dir": "send", "nack": true, "seq": ch.Seq, "reason": "out_of_window"})
		}
		return
	}

	before := n.Log.CountPerKey()
	for _, e := range ch.Entries {
		for _, op := range e.Ops {
			n.Log.Append(op)
		}
	}
	after := n.Log.CountPerKey()
	n.emit(EventAppliedDelta, map[string]any{"keys": len(ch.Entries), "before": before, "after": after})

	n.sessMu.Lock()
	n.sess.rcvExpect++
	n.sessMu.Unlock()

	_ = n.sendTo(from, wire.MT_SYNC_ACK, syncproto.EncodeAck(syncproto.Ack{Seq: ch.Seq}))
	n.emit(EventAck, map[string]any{"dir": "send", "seq": ch.Seq})

	if ch.Last && n.maybeEndIfEqual(from) {
		return
	}
	if ch.Last {
		n.sessMu.Lock()
		haveRoot := n.sess.haveRoot
		peerRoot := n.sess.lastRoot
		n.sessMu.Unlock()

		if haveRoot && n.computeRootFromLog() == peerRoot {
			n.endSessionTo(from, true, "caught_up_root")
			return
		}
	}
}

func (n *Node) onAck(_ transport.MemAddr, b []byte) {
	a, err := syncproto.DecodeAck(b)
	if err != nil {
		n.emit(EventWarn, map[string]any{"msg": "decode_ack_err", "err": err.Error()})
		return
	}

	n.emit(EventAck, map[string]any{"dir": "recv", "seq": a.Seq})

	n.sessMu.Lock()
	if n.sess.ackCh != nil {
		select {
		case n.sess.ackCh <- a.Seq:
		default:
		}
	}
	if a.Seq+1 > n.sess.sndNext {
		n.sess.sndNext = a.Seq + 1
	}
	n.sessMu.Unlock()

}

func (n *Node) streamChunks(to transport.MemAddr, chunks []syncproto.DeltaChunk, ackCh chan uint32, sessCtx context.Context) {
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
	snapshotInflight := func() {
		n.sessMu.Lock()
		n.sess.sndInflight = make([]syncproto.DeltaChunk, 0, len(unacked))
		for _, inf := range unacked {
			n.sess.sndInflight = append(n.sess.sndInflight, inf.ch)
		}
		n.sessMu.Unlock()
	}

	// Helper to send one chunk
	sendOne := func(ch syncproto.DeltaChunk) bool {
		enc := syncproto.EncodeDeltaChunk(ch)
		n.emit(EventSendDeltaChunk, map[string]any{
			"seq": ch.Seq, "entries": len(ch.Entries), "bytes": len(enc), "last": ch.Last,
		})
		if err := n.sendTo(to, wire.MT_SYNC_DELTA_CHUNK, enc); err != nil {
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

		case seq := <-ackCh:
			// Ack always increases by 1 (receiver is in-order)
			delete(unacked, seq)
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
						"seq": inf.ch.Seq, "entries": len(inf.ch.Entries), "bytes": len(enc), "last": inf.ch.Last, "retrans": true,
					})
					if err := n.sendTo(to, wire.MT_SYNC_DELTA_CHUNK, enc); err != nil {
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

func (n *Node) onRoot(from transport.MemAddr, b []byte) {
	r, err := syncproto.DecodeRoot(b)
	if err != nil {
		n.emit(EventWarn, map[string]any{"msg": "decode_root_err", "err": err.Error()})
		return
	}

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
	_ = n.sendTo(from, wire.MT_DESCENT_REQ, syncproto.EncodeDescentReq(req))
}

func (n *Node) onDescentReq(from transport.MemAddr, b []byte) {
	if !n.DescentEnabled {
		return
	}
	req, err := syncproto.DecodeDescentReq(b)
	if err != nil {
		n.emit(EventWarn, map[string]any{"msg": "decode_descent_req_err", "err": err.Error()})
		return
	}
	all := n.sortedLeaves()
	span := leavesWithPrefix(all, req.Prefix)

	// If small, return leaf list
	if len(span) <= n.DescentLeafK {
		out := make([]syncproto.LeafHash, 0, len(span))
		for _, lf := range span {
			out = append(out, syncproto.LeafHash{Key: lf.Key, Hash: lf.Hash})
		}
		resp := syncproto.DescentResp{Prefix: slices.Clone(req.Prefix), Leaves: out}

		n.emit(EventDescent, map[string]any{"dir": "<-", "kind": "resp_leaves", "prefix": string(req.Prefix), "leaves": len(out)})
		_ = n.sendTo(from, wire.MT_DESCENT_RESP, syncproto.EncodeDescentResp(resp))
		return
	}

	// Otherwise return child hashes
	groups := groupChildren(span, req.Prefix)
	kids := make([]syncproto.ChildHash, 0, len(groups))
	for lb, g := range groups {
		h := merkle.Build(g)
		kids = append(kids, syncproto.ChildHash{Label: lb, Hash: h, Count: uint32(len(g))})
	}
	// stable order
	slices.SortFunc(kids, func(a, b syncproto.ChildHash) int {
		if a.Label < b.Label {
			return -1
		}
		if a.Label > b.Label {
			return 1
		}
		return 0
	})
	resp := syncproto.DescentResp{Prefix: slices.Clone(req.Prefix), Children: kids}
	n.emit(EventDescent, map[string]any{"dir": "<-", "kind": "resp_children", "prefix": string(req.Prefix), "children": len(kids)})
	_ = n.sendTo(from, wire.MT_DESCENT_RESP, syncproto.EncodeDescentResp(resp))
}

func (n *Node) onDescentResp(from transport.MemAddr, b []byte) {
	if !n.DescentEnabled {
		return
	}
	resp, err := syncproto.DecodeDescentResp(b)
	if err != nil {
		n.emit(EventWarn, map[string]any{"msg": "decode_descent_resp_err", "err": err.Error()})
		return
	}

	// compute local view once
	all := n.sortedLeaves()

	if len(resp.Children) > 0 {
		// compare each child hash; queue the ones that differ
		for _, ch := range resp.Children {
			p := append(slices.Clone(resp.Prefix), ch.Label)
			// local hash for this child
			span := leavesWithPrefix(all, p)
			lh := merkle.Build(span)
			if lh != ch.Hash {
				n.sessMu.Lock()
				n.sess.dqueue = append(n.sess.dqueue, p)
				n.sessMu.Unlock()
			}
		}
		// pop next prefix and ask again
		n.sessMu.Lock()
		var next []byte
		if len(n.sess.dqueue) > 0 {
			next = n.sess.dqueue[0]
			n.sess.dqueue = n.sess.dqueue[1:]
		}
		n.sessMu.Unlock()
		if next != nil {
			n.emit(EventDescent, map[string]any{"dir": "->", "kind": "req_next", "prefix": string(next)})
			_ = n.sendTo(from, wire.MT_DESCENT_REQ, syncproto.EncodeDescentReq(syncproto.DescentReq{Prefix: next}))
		}
		return
	}

	if len(resp.Leaves) > 0 {
		// build precise Needs for keys whose leaf hash differs
		local := make(map[string][32]byte, len(resp.Leaves))
		for _, lf := range leavesWithPrefix(all, resp.Prefix) {
			local[lf.Key] = lf.Hash
		}
		counts := n.Log.CountPerKey()
		needs := make([]syncproto.Need, 0, len(resp.Leaves))
		for _, rlf := range resp.Leaves {
			if local[rlf.Key] != rlf.Hash {
				fromCnt := uint32(0)
				if c, ok := counts[rlf.Key]; ok && c > 0 {
					if c > int(^uint32(0)) {
						fromCnt = ^uint32(0)
					} else {
						fromCnt = uint32(c)
					}
				}
				needs = append(needs, syncproto.Need{Key: rlf.Key, From: fromCnt})
			}
		}
		if len(needs) > 0 {
			req := syncproto.Req{Needs: needs}
			n.emit(EventSendReq, map[string]any{"needs": req.Needs, "reason": "descent"})
			_ = n.sendTo(from, wire.MT_SYNC_REQ, syncproto.EncodeReq(req))
		}

		// continue with next queued prefix if any
		n.sessMu.Lock()
		var next []byte
		if len(n.sess.dqueue) > 0 {
			next = n.sess.dqueue[0]
			n.sess.dqueue = n.sess.dqueue[1:]
		}
		n.sessMu.Unlock()
		if next != nil {
			_ = n.sendTo(from, wire.MT_DESCENT_REQ, syncproto.EncodeDescentReq(syncproto.DescentReq{Prefix: next}))
		}
		return
	}
}

func (n *Node) sortedLeaves() []merkle.Leaf {
	snap := materialize.Snapshot(n.Log)
	ls := materialize.LeavesFromSnapshot(snap)
	sortLeaves(ls)
	return ls
}

func hasPrefix(s string, p []byte) bool {
	if len(p) > len(s) {
		return false
	}
	for i := range p {
		if s[i] != p[i] {
			return false
		}
	}
	return true
}

// leavesWithPrefix: O(n) scan — fine for now
func leavesWithPrefix(all []merkle.Leaf, p []byte) []merkle.Leaf {
	out := make([]merkle.Leaf, 0, 64)
	for _, lf := range all {
		if hasPrefix(lf.Key, p) {
			out = append(out, lf)
		}
	}
	return out
}

// groupChildren: group span by next byte after prefix; return label->slice
func groupChildren(span []merkle.Leaf, p []byte) map[byte][]merkle.Leaf {
	m := make(map[byte][]merkle.Leaf)
	plen := len(p)
	for _, lf := range span {
		if len(lf.Key) <= plen {
			// exact match — treat as its own tiny span; we’ll hit K-threshold anyway
			// (no child label; will be covered when span size<=K)
			continue
		}
		lb := lf.Key[plen]
		m[lb] = append(m[lb], lf)
	}
	return m
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
	n.sess.sndInflight = nil
	n.sess.rcvExpect = 0
	n.sess.ackCh = make(chan uint32, 8)
	n.sess.ctx, n.sess.cancel = context.WithCancel(n.ctx)
	n.emit(EventSync, map[string]any{"action": "begin", "id": n.sess.id})
}

func (n *Node) endSessionTo(to transport.MemAddr, sendEnd bool, reason string) {
	n.sessMu.Lock()
	defer n.sessMu.Unlock()
	if !n.sess.active {
		return
	}
	if sendEnd {
		_ = n.sendTo(to, wire.MT_SYNC_END, nil)
	}
	if n.sess.cancel != nil {
		n.sess.cancel()
	}
	n.emit(EventSync, map[string]any{"action": "end", "id": n.sess.id, "reason": reason})
	n.sess = syncSession{}
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

func (n *Node) onDeltaNack(from transport.MemAddr, b []byte) {
	nack, err := syncproto.DecodeDeltaNack(b)
	if err != nil {
		n.emit(EventWarn, map[string]any{"msg": "decode_delta_nack_err", "err": err.Error()})
		return
	}
	n.emit(EventAck, map[string]any{"dir": "recv", "nack": true, "seq": nack.Seq, "code": nack.Code})

	// We keep the last sent chunk in sess.sndInflight (window is small).
	n.sessMu.Lock()
	infl := n.sess.sndInflight
	n.sessMu.Unlock()
	if len(infl) == 0 {
		n.emit(EventWarn, map[string]any{"msg": "nack_no_inflight"})
		return
	}
	for _, ch := range infl {
		if ch.Seq != nack.Seq {
			continue
		}
		enc := syncproto.EncodeDeltaChunk(ch)
		n.emit(EventSendDeltaChunk, map[string]any{
			"seq": ch.Seq, "entries": len(ch.Entries), "bytes": len(enc), "last": ch.Last, "retrans": true, "reason": "nack",
		})
		if err := n.sendTo(from, wire.MT_SYNC_DELTA_CHUNK, enc); err != nil {
			n.emit(EventWarn, map[string]any{"msg": "retransmit_err", "seq": ch.Seq, "err": err.Error()})
		}
		return
	}
	// No match in inflight (could be stale); log and move on.
	n.emit(EventWarn, map[string]any{"msg": "nack_unknown_seq", "seq": nack.Seq})
}
