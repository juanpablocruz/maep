package engine

import (
	"bytes"
	"testing"
)

func Test_Frame_Decode(t *testing.T) {
	f := Frame{
		Header: FrameHeader{
			Magic:    MAGIC,
			Version:  1,
			Type:     FrameTypeSummaryReq,
			Reserved: 0,
			Epoch:    1234567890,
			HLC: HLCTimestamp{
				wallns:  1234567890,
				logical: 12345,
			},
			Seq:        12345,
			Actor:      ActorID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			Sender:     PeerID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			Dest:       PeerID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			PayloadLen: 12345,
		},
		Payload: []byte("payload"),
	}

	enc, err := f.Encode()
	if err != nil {
		t.Errorf("error encoding: %v", err)
	}
	t.Logf("enc: %x", enc)

	dec, err := DecodeFrame(enc)
	if err != nil {
		t.Errorf("error decoding: %v", err)
	}
	t.Logf("dec: %v", dec)

	if !bytes.Equal(f.Header.Magic[:], dec.Header.Magic[:]) {
		t.Errorf("magic should be equal")
	}
	if f.Header.Version != dec.Header.Version {
		t.Errorf("version should be equal")
	}
	if f.Header.Type != dec.Header.Type {
		t.Errorf("type should be equal")
	}
	if f.Header.Reserved != dec.Header.Reserved {
		t.Errorf("reserved should be equal")
	}
	if f.Header.Epoch != dec.Header.Epoch {
		t.Errorf("epoch should be equal")
	}
	if f.Header.HLC.wallns != dec.Header.HLC.wallns {
		t.Errorf("wall_ns should be equal")
	}
	if f.Header.HLC.logical != dec.Header.HLC.logical {
		t.Errorf("logical should be equal")
	}
	if f.Header.Seq != dec.Header.Seq {
		t.Errorf("seq should be equal")
	}
	if !bytes.Equal(f.Header.Actor[:], dec.Header.Actor[:]) {
		t.Errorf("actor should be equal")
	}
	if !bytes.Equal(f.Header.Sender[:], dec.Header.Sender[:]) {
		t.Errorf("sender should be equal")
	}
	if !bytes.Equal(f.Header.Dest[:], dec.Header.Dest[:]) {
		t.Errorf("dest should be equal")
	}
	if f.Header.PayloadLen != dec.Header.PayloadLen {
		t.Errorf("payload length should be equal")
	}
	if !bytes.Equal(f.Payload, dec.Payload) {
		t.Errorf("payload should be equal")
	}
}

func Test_Frame_Encode_TooLarge(t *testing.T) {
	f := Frame{
		Header: FrameHeader{
			Magic:    MAGIC,
			Version:  1,
			Type:     FrameTypeSummaryReq,
			Reserved: 0,
			Epoch:    1234567890,
			HLC: HLCTimestamp{
				wallns:  1234567890,
				logical: 12345,
			},
			Seq:        12345,
			Actor:      ActorID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			Sender:     PeerID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			Dest:       PeerID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			PayloadLen: uint32(MaxPayloadLen + 1),
		},
		Payload: make([]byte, MaxPayloadLen+1),
	}

	_, err := f.Encode()
	if err != ErrFrameTooLarge {
		t.Errorf("error should be ErrFrameTooLarge")
	}
}
func Test_Frame_Decode_InvalidMagic(t *testing.T) {
	f := Frame{
		Header: FrameHeader{
			Magic:    [4]byte{0x33, 0x33, 0x33, 0x33},
			Version:  1,
			Type:     FrameTypeSummaryReq,
			Reserved: 0,
			Epoch:    1234567890,
			HLC: HLCTimestamp{
				wallns:  1234567890,
				logical: 12345,
			},
			Seq:        12345,
			Actor:      ActorID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			Sender:     PeerID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			Dest:       PeerID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			PayloadLen: 12345,
		},
		Payload: []byte("payload"),
	}

	b, err := f.Encode()
	if err != nil {
		t.Errorf("error encoding: %v", err)
	}
	t.Logf("enc: %x", b)

	_, err = DecodeFrame(b)
	if err != ErrFrameInvalidMagic {
		t.Errorf("error should be ErrFrameInvalidMagic, got: %v", err)
	}
}
func Test_Frame_Decode_InvalidReserved(t *testing.T) {
	f := Frame{
		Header: FrameHeader{
			Magic:    MAGIC,
			Version:  1,
			Type:     FrameTypeSummaryReq,
			Reserved: 1,
			Epoch:    1234567890,
			HLC: HLCTimestamp{
				wallns:  1234567890,
				logical: 12345,
			},
			Seq:        12345,
			Actor:      ActorID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			Sender:     PeerID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			Dest:       PeerID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			PayloadLen: 12345,
		},
		Payload: []byte("payload"),
	}

	b, err := f.Encode()
	if err != nil {
		t.Errorf("error encoding: %v", err)
	}
	t.Logf("enc: %x", b)

	_, err = DecodeFrame(b)
	if err != ErrFrameInvalidReserved {
		t.Errorf("error should be ErrFrameInvalidReserved")
	}
}

func Test_Frame_Decode_InvalidVersion(t *testing.T) {
	f := Frame{
		Header: FrameHeader{
			Magic:    MAGIC,
			Version:  2,
			Type:     FrameTypeSummaryReq,
			Reserved: 0,
			Epoch:    1234567890,
			HLC: HLCTimestamp{
				wallns:  1234567890,
				logical: 12345,
			},
			Seq:        12345,
			Actor:      ActorID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			Sender:     PeerID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			Dest:       PeerID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			PayloadLen: 12345,
		},
		Payload: []byte("payload"),
	}

	b, err := f.Encode()
	if err != nil {
		t.Errorf("error encoding: %v", err)
	}
	t.Logf("enc: %x", b)

	_, err = DecodeFrame(b)
	if err != ErrFrameInvalidVersion {
		t.Errorf("error should be ErrFrameInvalidVersion")
	}
}

func Test_Frame_Round_Trip(t *testing.T) {
	f := Frame{
		Header: FrameHeader{
			Magic:    MAGIC,
			Version:  1,
			Type:     FrameTypeSummaryReq,
			Reserved: 0,
			Epoch:    1234567890,
			HLC: HLCTimestamp{
				wallns:  1234567890,
				logical: 12345,
			},
			Seq:        12345,
			Actor:      ActorID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			Sender:     PeerID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			Dest:       PeerID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			PayloadLen: 12345,
		},
		Payload: []byte("payload"),
	}

	b, err := f.Encode()
	if err != nil {
		t.Errorf("error encoding: %v", err)
	}
	t.Logf("enc: %x", b)

	dec, err := DecodeFrame(b)
	if err != nil {
		t.Errorf("error decoding: %v", err)
	}
	t.Logf("dec: %v", dec)

	enc2, err := dec.Encode()
	if err != nil {
		t.Errorf("error encoding: %v", err)
	}
	t.Logf("enc2: %x", enc2)

	if !bytes.Equal(b, enc2) {
		t.Errorf("encoded frames should be equal")
	}
}
