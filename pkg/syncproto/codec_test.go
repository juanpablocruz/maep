package syncproto

import "testing"

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
