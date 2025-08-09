package syncproto

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
)

type DescentReq struct {
	Prefix []byte
}

type ChildHash struct {
	Label byte
	Hash  [32]byte
	Count uint32
}

type LeafHash struct {
	Key  string
	Hash [32]byte
}

type DescentResp struct {
	Prefix   []byte
	Children []ChildHash
	Leaves   []LeafHash
}

func EncodeDescentReq(r DescentReq) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, uint16(len(r.Prefix)))
	buf.Write(r.Prefix)
	return buf.Bytes()
}

func DecodeDescentReq(b []byte) (DescentReq, error) {
	if len(b) < 2 {
		return DescentReq{}, errors.New("short descent req")
	}
	plen := int(binary.BigEndian.Uint16(b[:2]))

	if len(b) < 2+plen {
		return DescentReq{}, errors.New("short descent req prefix")
	}
	return DescentReq{Prefix: slices.Clone(b[2 : 2+plen])}, nil
}

func EncodeDescentResp(r DescentResp) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, uint16(len(r.Prefix)))
	buf.Write(r.Prefix)

	_ = binary.Write(&buf, binary.BigEndian, uint16(len(r.Children)))
	for _, c := range r.Children {
		buf.WriteByte(c.Label)
		buf.Write(c.Hash[:])
		_ = binary.Write(&buf, binary.BigEndian, c.Count)
	}

	_ = binary.Write(&buf, binary.BigEndian, uint16(len(r.Leaves)))
	for _, l := range r.Leaves {
		_ = binary.Write(&buf, binary.BigEndian, uint16(len(l.Key)))
		buf.WriteString(l.Key)
		buf.Write(l.Hash[:])
	}
	return buf.Bytes()
}

func DecodeDescentResp(b []byte) (DescentResp, error) {
	r := DescentResp{}
	if len(b) < 2 {
		return r, errors.New("short descent resp")
	}
	plen := int(binary.BigEndian.Uint16(b[:2]))
	b = b[2:]
	if len(b) < plen {
		return r, errors.New("short prefix")
	}
	r.Prefix = slices.Clone(b[:plen])
	b = b[plen:]

	if len(b) < 2 {
		return r, errors.New("short children len")
	}
	nChild := int(binary.BigEndian.Uint16(b[:2]))
	b = b[2:]
	r.Children = make([]ChildHash, nChild)
	for i := range nChild {
		if len(b) < 1+32+4 {
			return r, errors.New("short child")
		}
		r.Children[i].Label = b[0]
		copy(r.Children[i].Hash[:], b[1:33])
		r.Children[i].Count = binary.BigEndian.Uint32(b[33:37])
		b = b[37:]
	}

	if len(b) < 2 {
		return r, errors.New("short leaves len")
	}
	nLeaves := int(binary.BigEndian.Uint16(b[:2]))
	b = b[2:]
	r.Leaves = make([]LeafHash, nLeaves)
	for i := range nLeaves {
		if len(b) < 2 {
			return r, errors.New("short leaf key len")
		}
		klen := int(binary.BigEndian.Uint16(b[:2]))
		b = b[2:]
		if len(b) < klen+32 {
			return r, errors.New("short leaf")
		}
		r.Leaves[i].Key = string(b[:klen])
		copy(r.Leaves[i].Hash[:], b[klen:klen+32])
		b = b[klen+32:]
	}
	return r, nil
}
