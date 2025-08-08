package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const (
	MT_SYNC_SUMMARY byte = 0x01
	MT_SYNC_REQ     byte = 0x02
	MT_SYNC_DELTA   byte = 0x03

	MT_SYNC_BEGIN byte = 0x04
	MT_SYNC_END   byte = 0x05

	MT_PING byte = 0x10
	MT_PONG byte = 0x11

	MT_SEG_AD byte = 0x12
)

// Encode frame: | 1B type | 4B big-endian length | payload... |
func Encode(mt byte, payload []byte) []byte {
	var buf bytes.Buffer
	buf.WriteByte(mt)
	var L [4]byte
	binary.BigEndian.PutUint32(L[:], uint32(len(payload)))
	buf.Write(L[:])
	buf.Write(payload)
	return buf.Bytes()
}

// Decode validates and returns (type, payload).
func Decode(frame []byte) (byte, []byte, error) {
	if len(frame) < 5 {
		return 0, nil, errors.New("short frame")
	}
	mt := frame[0]
	L := binary.BigEndian.Uint32(frame[1:5])
	if int(5+L) != len(frame) {
		return 0, nil, errors.New("length mismatch")
	}
	return mt, frame[5:], nil
}

func PutU16(b *bytes.Buffer, v uint16) {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], v)
	b.Write(tmp[:])
}

func PutU32(b *bytes.Buffer, v uint32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	b.Write(tmp[:])
}

func PutU64(b *bytes.Buffer, v uint64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], v)
	b.Write(tmp[:])
}

func GetU16(r *bytes.Reader) (uint16, error) {
	var tmp [2]byte
	if _, err := r.Read(tmp[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(tmp[:]), nil
}

func GetU32(r *bytes.Reader) (uint32, error) {
	var tmp [4]byte
	if _, err := r.Read(tmp[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(tmp[:]), nil
}

func GetU64(r *bytes.Reader) (uint64, error) {
	var tmp [8]byte
	if _, err := r.Read(tmp[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(tmp[:]), nil
}
