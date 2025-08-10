package protoport

import (
	"context"

	"github.com/juanpablocruz/maep/pkg/syncproto"
	"github.com/juanpablocruz/maep/pkg/transport"
	"github.com/juanpablocruz/maep/pkg/wire"
)

// Peer is the transport address
type Peer = transport.MemAddr

// Message is a typed union of wire-level messages used by sync.
// For now we wrap the existing syncproto types.
type Message interface{ isMessage() }

type (
	SummaryRespMsg struct{ S syncproto.Summary }
	SummaryReqMsg  struct{ R syncproto.SummaryReq }
	ReqMsg         struct{ R syncproto.Req }
	DeltaMsg       struct{ D syncproto.Delta }
	DeltaChunkMsg  struct{ C syncproto.DeltaChunk }
	AckMsg         struct{ A syncproto.Ack }
	DeltaNackMsg   struct{ N syncproto.DeltaNack }
	SyncBeginMsg   struct{}
	RootMsg        struct{ R syncproto.Root }
	DescentReqMsg  struct{ R syncproto.DescentReq }
	DescentRespMsg struct{ R syncproto.DescentResp }
	SegAdMsg       struct{ A syncproto.SegAd }
	SegKeysReqMsg  struct{ R syncproto.SegKeysReq }
	SegKeysMsg     struct{ K syncproto.SegKeys }
	PingMsg        struct{}
	PongMsg        struct{}
	// duplicate declaration removed
)

func (SummaryRespMsg) isMessage() {}
func (SummaryReqMsg) isMessage()  {}
func (ReqMsg) isMessage()         {}
func (DeltaMsg) isMessage()       {}
func (DeltaChunkMsg) isMessage()  {}
func (AckMsg) isMessage()         {}
func (DeltaNackMsg) isMessage()   {}
func (SyncBeginMsg) isMessage()   {}
func (RootMsg) isMessage()        {}
func (DescentReqMsg) isMessage()  {}
func (DescentRespMsg) isMessage() {}
func (SegAdMsg) isMessage()       {}
func (SegKeysReqMsg) isMessage()  {}
func (SegKeysMsg) isMessage()     {}
func (PingMsg) isMessage()        {}
func (PongMsg) isMessage()        {}

// Messenger abstracts sending/receiving typed messages.
type Messenger interface {
	Recv(ctx context.Context) (from Peer, m Message, ok bool)
	Send(ctx context.Context, to Peer, m Message) error
}

// WireMessenger adapts EndpointIF and wire codec to Messenger.
type WireMessenger struct {
	EP transport.EndpointIF
}

func (wm WireMessenger) Recv(ctx context.Context) (Peer, Message, bool) {
	if fe, ok := wm.EP.(transport.FromEndpoint); ok {
		from, frame, ok2 := fe.RecvFrom(ctx)
		if !ok2 {
			return "", nil, false
		}
		m, ok3 := decodeFrame(frame)
		return from, m, ok3
	}
	frame, ok := wm.EP.Recv(ctx)
	if !ok {
		return "", nil, false
	}
	m, ok3 := decodeFrame(frame)
	return wm.EP.Addr(), m, ok3
}

func (wm WireMessenger) Send(ctx context.Context, to Peer, m Message) error {
	mt, payload, ok := EncodeMessage(m)
	if !ok {
		return nil
	}
	frame := wire.Encode(mt, payload)
	return wm.EP.Send(to, frame)
}

// EncodeMessage maps a typed Message to (mt, payload). Returns ok=false if unknown type.
func EncodeMessage(m Message) (mt byte, payload []byte, ok bool) {
	switch x := m.(type) {
	case SummaryRespMsg:
		return wire.MT_SYNC_SUMMARY_RESP, syncproto.EncodeSummary(x.S), true
	case SummaryReqMsg:
		return wire.MT_SYNC_SUMMARY_REQ, syncproto.EncodeSummaryReq(x.R), true
	case ReqMsg:
		return wire.MT_SYNC_REQ, syncproto.EncodeReq(x.R), true
	case DeltaMsg:
		return wire.MT_SYNC_DELTA, syncproto.EncodeDelta(x.D), true
	case DeltaChunkMsg:
		return wire.MT_SYNC_DELTA_CHUNK, syncproto.EncodeDeltaChunk(x.C), true
	case AckMsg:
		return wire.MT_SYNC_ACK, syncproto.EncodeAck(x.A), true
	case DeltaNackMsg:
		return wire.MT_DELTA_NACK, syncproto.EncodeDeltaNack(x.N), true
	case SyncBeginMsg:
		return wire.MT_SYNC_BEGIN, nil, true
	case RootMsg:
		return wire.MT_SYNC_ROOT, syncproto.EncodeRoot(x.R), true
	case DescentReqMsg:
		return wire.MT_DESCENT_REQ, syncproto.EncodeDescentReq(x.R), true
	case DescentRespMsg:
		return wire.MT_DESCENT_RESP, syncproto.EncodeDescentResp(x.R), true
	case SegAdMsg:
		return wire.MT_SEG_AD, syncproto.EncodeSegAd(x.A), true
	case SegKeysReqMsg:
		return wire.MT_SEG_KEYS_REQ, syncproto.EncodeSegKeysReq(x.R), true
	case SegKeysMsg:
		return wire.MT_SEG_KEYS, syncproto.EncodeSegKeys(x.K), true
	case PingMsg:
		return wire.MT_PING, nil, true
	case PongMsg:
		return wire.MT_PONG, nil, true
	default:
		return 0, nil, false
	}
}

func decodeFrame(frame []byte) (Message, bool) {
	mt, payload, err := wire.Decode(frame)
	if err != nil {
		return nil, false
	}
	switch mt {
	case wire.MT_SYNC_SUMMARY_RESP:
		s, err := syncproto.DecodeSummary(payload)
		if err != nil {
			return nil, false
		}
		return SummaryRespMsg{S: s}, true
	case wire.MT_SYNC_SUMMARY_REQ:
		r, err := syncproto.DecodeSummaryReq(payload)
		if err != nil {
			return nil, false
		}
		return SummaryReqMsg{R: r}, true
	case wire.MT_SYNC_REQ:
		r, err := syncproto.DecodeReq(payload)
		if err != nil {
			return nil, false
		}
		return ReqMsg{R: r}, true
	case wire.MT_SYNC_DELTA:
		d, err := syncproto.DecodeDelta(payload)
		if err != nil {
			return nil, false
		}
		return DeltaMsg{D: d}, true
	case wire.MT_SYNC_DELTA_CHUNK:
		c, err := syncproto.DecodeDeltaChunk(payload)
		if err != nil {
			return nil, false
		}
		return DeltaChunkMsg{C: c}, true
	case wire.MT_SYNC_ACK:
		a, err := syncproto.DecodeAck(payload)
		if err != nil {
			return nil, false
		}
		return AckMsg{A: a}, true
	case wire.MT_DELTA_NACK:
		n, err := syncproto.DecodeDeltaNack(payload)
		if err != nil {
			return nil, false
		}
		return DeltaNackMsg{N: n}, true
	case wire.MT_SYNC_ROOT:
		r, err := syncproto.DecodeRoot(payload)
		if err != nil {
			return nil, false
		}
		return RootMsg{R: r}, true
	case wire.MT_DESCENT_REQ:
		r, err := syncproto.DecodeDescentReq(payload)
		if err != nil {
			return nil, false
		}
		return DescentReqMsg{R: r}, true
	case wire.MT_DESCENT_RESP:
		r, err := syncproto.DecodeDescentResp(payload)
		if err != nil {
			return nil, false
		}
		return DescentRespMsg{R: r}, true
	case wire.MT_SEG_AD:
		a, err := syncproto.DecodeSegAd(payload)
		if err != nil {
			return nil, false
		}
		return SegAdMsg{A: a}, true
	case wire.MT_SEG_KEYS_REQ:
		r, err := syncproto.DecodeSegKeysReq(payload)
		if err != nil {
			return nil, false
		}
		return SegKeysReqMsg{R: r}, true
	case wire.MT_SEG_KEYS:
		k, err := syncproto.DecodeSegKeys(payload)
		if err != nil {
			return nil, false
		}
		return SegKeysMsg{K: k}, true
	case wire.MT_PING:
		return PingMsg{}, true
	case wire.MT_PONG:
		return PongMsg{}, true
	case wire.MT_SYNC_END:
		return SummaryReqMsg{}, true // placeholder to avoid unknown type; node handles END on wire path
	default:
		return nil, false
	}
}
