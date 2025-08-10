package bin

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

func PutU16(b *bytes.Buffer, v uint16) error { return binary.Write(b, binary.BigEndian, v) }
func PutU32(b *bytes.Buffer, v uint32) error { return binary.Write(b, binary.BigEndian, v) }
func PutU64(b *bytes.Buffer, v uint64) error { return binary.Write(b, binary.BigEndian, v) }

func GetU16(r *bytes.Reader) (uint16, error) {
	var v uint16
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}
func GetU32(r *bytes.Reader) (uint32, error) {
	var v uint32
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}
func GetU64(r *bytes.Reader) (uint64, error) {
	var v uint64
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

func PutBytes(b *bytes.Buffer, p []byte) error {
	if err := PutU32(b, uint32(len(p))); err != nil {
		return err
	}
	if _, err := b.Write(p); err != nil {
		return err
	}
	return nil
}

func GetBytes(r *bytes.Reader) ([]byte, error) {
	n32, err := GetU32(r)
	if err != nil {
		return nil, err
	}
	if uint64(n32) > uint64(r.Len()) {
		return nil, io.ErrUnexpectedEOF
	}
	maxInt := int(^uint(0) >> 1)
	if uint64(n32) > uint64(maxInt) {
		return nil, errors.New("length too large")
	}
	n := int(n32)
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
