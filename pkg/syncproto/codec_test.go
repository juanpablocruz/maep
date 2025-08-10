package syncproto

import (
	"testing"

	"github.com/juanpablocruz/maep/pkg/model"
)

func TestReqRoundtrip(t *testing.T) {
	r := Req{Needs: []Need{{Key: "A", From: 0}, {Key: "B", From: 2}}}
	b := EncodeReq(r)
	got, err := DecodeReq(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Needs) != 2 || got.Needs[1].Key != "B" || got.Needs[1].From != 2 {
		t.Fatalf("bad roundtrip: %#v", got)
	}
}

func TestSummaryWithFrontierRoundtrip(t *testing.T) {
	s := Summary{
		Root:     [32]byte{1, 2, 3},
		Leaves:   []LeafSummary{{Key: "k1"}, {Key: "k2"}},
		Frontier: []FrontierItem{{Key: "k1", From: 3}, {Key: "k2", From: 5}},
	}
	b := EncodeSummary(s)
	got, err := DecodeSummary(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Leaves) != 2 || len(got.Frontier) != 2 {
		t.Fatalf("bad leaves/frontier len: %#v", got)
	}
	if got.Frontier[0].Key != "k1" || got.Frontier[0].From != 3 {
		t.Fatalf("bad frontier[0]: %#v", got.Frontier[0])
	}
}

func TestDeltaChunkHashValidation(t *testing.T) {
	c := DeltaChunk{SID: 0, Seq: 1, Last: true, Entries: []DeltaEntry{{Key: "k", Ops: []model.Op{{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "k"}}}}}
	enc := EncodeDeltaChunk(c)
	if len(enc) < 32 {
		t.Fatal("encoded too short")
	}
	// Corrupt last byte of trailing hash
	enc[len(enc)-1] ^= 0xFF
	if _, err := DecodeDeltaChunk(enc); err == nil {
		t.Fatalf("expected error on bad chunk hash")
	}
}
