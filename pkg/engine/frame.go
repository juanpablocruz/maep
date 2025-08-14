package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type FrameType byte

const (
	FrameTypeSummaryReq FrameType = iota
	FrameTypeSummaryResp
	FrameTypeLeaves
	FrameTypeDelta
	FrameTypeAck
	FrameTypeCsrPrepare
	FrameTypeCsrCommit
	FrameTypeCsrAbort
	FrameTypeJoin
	FrameTypeLeave
	FrameTypePing
	FrameTypePong
)

var (
	ErrFrameTooLarge        = fmt.Errorf("frame too large")
	ErrFrameInvalidMagic    = fmt.Errorf("invalid magic")
	ErrFrameInvalidType     = fmt.Errorf("invalid type")
	ErrFrameInvalidReserved = fmt.Errorf("invalid reserved")
	ErrFrameInvalidActor    = fmt.Errorf("invalid actor")
	ErrFrameInvalidVersion  = fmt.Errorf("invalid version")
)

var MAGIC = [4]byte{0x4D, 0x41, 0x45, 0x50}
var MaxPayloadLen = uint32(1024 * 1024) // 1 MB

type PeerID [16]byte
type FrameHeader struct {
	Magic      [4]byte
	Version    byte
	Type       FrameType
	Reserved   byte
	Epoch      uint64
	HLC        HLCTimestamp
	Seq        uint32
	Actor      ActorID
	Sender     PeerID
	Dest       PeerID
	PayloadLen uint32
}

// Frame is the envelope that is sent/received over the wire.
type Frame struct {
	Header  FrameHeader
	Payload []byte
}

func (f *Frame) Encode() ([]byte, error) {
	var buf bytes.Buffer

	buf.Write(f.Header.Magic[:])
	buf.WriteByte(f.Header.Version)
	buf.WriteByte(byte(f.Header.Type))
	buf.WriteByte(f.Header.Reserved)
	binary.Write(&buf, binary.BigEndian, f.Header.Epoch)
	binary.Write(&buf, binary.BigEndian, f.Header.HLC.wallns)
	binary.Write(&buf, binary.BigEndian, f.Header.HLC.logical)
	binary.Write(&buf, binary.BigEndian, f.Header.Seq)
	buf.Write(f.Header.Actor[:])

	// Encode Sender (16 bytes)
	buf.Write(f.Header.Sender[:])

	// Encode Dest (16 bytes)
	buf.Write(f.Header.Dest[:])

	// Encode Payload length (4 bytes)
	// Len must be <= MAX_PAYLOAD_LEN
	if uint32(len(f.Payload)) > MaxPayloadLen {
		return nil, ErrFrameTooLarge
	}
	binary.Write(&buf, binary.BigEndian, f.Header.PayloadLen)

	// Encode Payload
	if len(f.Payload) > 0 {
		buf.Write(f.Payload)
	}
	return buf.Bytes(), nil
}

func DecodeFrame(data []byte) (*Frame, error) {
	f := Frame{}

	buf := bytes.NewBuffer(data)

	// Decode Magic (4 bytes)
	_, err := buf.Read(f.Header.Magic[:])
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(f.Header.Magic[:], MAGIC[:]) {
		return nil, ErrFrameInvalidMagic
	}

	// Decode Version (1 byte)
	f.Header.Version = buf.Next(1)[0]
	if f.Header.Version != 1 {
		return nil, ErrFrameInvalidVersion
	}

	// Decode Type (1 byte)
	f.Header.Type = FrameType(buf.Next(1)[0])

	// Decode Reserved (1 byte)
	f.Header.Reserved = buf.Next(1)[0]
	if f.Header.Reserved != 0 {
		return nil, ErrFrameInvalidReserved
	}

	// Decode Epoch (8 bytes)
	f.Header.Epoch = binary.BigEndian.Uint64(buf.Next(8))

	// Decode HLC timestamp (8 bytes wall_ns + 4 bytes logical, big endian)
	f.Header.HLC.wallns = binary.BigEndian.Uint64(buf.Next(8))
	f.Header.HLC.logical = binary.BigEndian.Uint32(buf.Next(4))

	// Decode Seq (4 bytes)
	f.Header.Seq = binary.BigEndian.Uint32(buf.Next(4))

	// Decode Actor (16 bytes)
	_, err = buf.Read(f.Header.Actor[:])
	if err != nil {
		return nil, err
	}

	// Decode Sender (16 bytes)
	_, err = buf.Read(f.Header.Sender[:])
	if err != nil {
		return nil, err
	}

	// Decode Dest (16 bytes)
	_, err = buf.Read(f.Header.Dest[:])
	if err != nil {
		return nil, err
	}

	// Decode Payload length (4 bytes)
	f.Header.PayloadLen = binary.BigEndian.Uint32(buf.Next(4))
	if f.Header.PayloadLen > MaxPayloadLen {
		return nil, ErrFrameTooLarge
	}

	// Decode Payload
	if f.Header.PayloadLen > 0 {
		f.Payload = buf.Next(int(f.Header.PayloadLen))
	}

	return &f, nil
}
