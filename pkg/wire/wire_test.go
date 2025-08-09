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

func TestPutGetUint(t *testing.T) {
	buf := &bytes.Buffer{}
	PutU16(buf, 0xABCD)
	PutU32(buf, 0x01020304)
	PutU64(buf, 0x0102030405060708)
	r := bytes.NewReader(buf.Bytes())
	u16, err := GetU16(r)
	if err != nil || u16 != 0xABCD {
		t.Fatalf("GetU16: %v %x", err, u16)
	}
	u32, err := GetU32(r)
	if err != nil || u32 != 0x01020304 {
		t.Fatalf("GetU32: %v %x", err, u32)
	}
	u64, err := GetU64(r)
	if err != nil || u64 != 0x0102030405060708 {
		t.Fatalf("GetU64: %v %x", err, u64)
	}
}

func TestDecodeErrors(t *testing.T) {
	if _, _, err := Decode([]byte{0x01}); err == nil {
		t.Fatalf("expected short frame error")
	}
	bad := []byte{0x01, 0, 0, 0, 10, 1, 2}
	if _, _, err := Decode(bad); err == nil {
		t.Fatalf("expected length mismatch error")
	}
}
