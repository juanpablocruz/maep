package protoport

import (
	"testing"

	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/syncproto"
	"github.com/juanpablocruz/maep/pkg/wire"
)

// Spec: Message encoding maps 1:1 to wire types
func TestEncodeMessageTypeMapping(t *testing.T) {
	var actor model.ActorID
	msgCases := []Message{
		SummaryRespMsg{S: syncproto.Summary{}},
		SummaryReqMsg{R: syncproto.SummaryReq{}},
		ReqMsg{R: syncproto.Req{}},
		DeltaMsg{D: syncproto.Delta{}},
		DeltaChunkMsg{C: syncproto.DeltaChunk{}},
		AckMsg{A: syncproto.Ack{}},
		DeltaNackMsg{N: syncproto.DeltaNack{}},
		SyncBeginMsg{}, RootMsg{R: syncproto.Root{}},
		DescentReqMsg{R: syncproto.DescentReq{}}, DescentRespMsg{R: syncproto.DescentResp{}},
		SegAdMsg{A: syncproto.SegAd{}}, SegKeysReqMsg{R: syncproto.SegKeysReq{}}, SegKeysMsg{K: syncproto.SegKeys{}},
		PingMsg{}, PongMsg{},
	}
	for _, m := range msgCases {
		mt, payload, ok := EncodeMessage(m)
		if !ok {
			t.Fatalf("encode failed for %T", m)
		}
		// Ensure decodes back to some message of expected kind
		decMt, decPl, err := wire.Decode(wire.Encode(mt, payload))
		if err != nil || decMt != mt || len(decPl) != len(payload) {
			t.Fatalf("roundtrip failed for %T: err=%v", m, err)
		}
		_ = actor // silence unused in case
	}
}
