package protocol_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/juanpablocruz/maepsim/internal/protocol"
)

func TestEncodeDecodeHeader(t *testing.T) {
	original := protocol.MessageHeader{
		Version: protocol.Version,
		Type:    protocol.MsgSync,
		Length:  12345,
	}
	encoded, err := protocol.EncodeHeader(original)
	if err != nil {
		t.Fatalf("EncodeHeader failed: %v", err)
	}
	if len(encoded) != 6 {
		t.Errorf("Expected encoded header length to be 6, got %d", len(encoded))
	}
	reader := bytes.NewReader(encoded)
	decoded, err := protocol.DecodeHeader(reader)
	if err != nil {
		t.Fatalf("DecodeHeader failed: %v", err)
	}
	if decoded != original {
		t.Errorf("Header mismatch: got %+v, want %+v", decoded, original)
	}
}

func TestDecodeHeaderInsufficientBytes(t *testing.T) {
	truncated := []byte{protocol.Version, protocol.MsgSync, 0x00}
	reader := bytes.NewReader(truncated)
	_, err := protocol.DecodeHeader(reader)
	if err == nil {
		t.Fatal("Expected error with insufficient bytes, got nil")
	}
}

func TestDecodeHeaderExtraData(t *testing.T) {
	original := protocol.MessageHeader{
		Version: protocol.Version,
		Type:    protocol.MsgJoin,
		Length:  999,
	}
	encoded, err := protocol.EncodeHeader(original)
	if err != nil {
		t.Fatalf("EncodeHeader failed: %v", err)
	}
	extraData := []byte{0xAA, 0xBB, 0xCC}
	combined := append(encoded, extraData...)
	reader := bytes.NewReader(combined)
	decoded, err := protocol.DecodeHeader(reader)
	if err != nil {
		t.Fatalf("DecodeHeader failed: %v", err)
	}
	remaining, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read remaining bytes: %v", err)
	}
	if !bytes.Equal(remaining, extraData) {
		t.Errorf("Extra bytes mismatch: got %v, want %v", remaining, extraData)
	}
	if decoded != original {
		t.Errorf("Header mismatch: got %+v, want %+v", decoded, original)
	}
}
