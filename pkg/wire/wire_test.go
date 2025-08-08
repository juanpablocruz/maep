package wire

import (
	"bytes"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	payload := []byte("hello")
	f := Encode(MT_PING, payload)
	mt, p, err := Decode(f)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if mt != MT_PING || !bytes.Equal(p, payload) {
		t.Fatalf("roundtrip mismatch: mt=%d p=%q", mt, p)
	}
}
