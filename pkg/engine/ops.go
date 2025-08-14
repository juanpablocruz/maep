package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
)

// An ActorID is a unique identifier for a node,
// it is assigned at node creation and never changes
type ActorID [16]byte

// An HLCTimestamp is a timestamp that is used to order operations
// it is stored in Big Endian format
type HLCTimestamp struct {
	wallns  uint64
	logical uint32
}

func NewHLC(wallns uint64, logical uint32) HLCTimestamp {
	return HLCTimestamp{
		wallns:  wallns,
		logical: logical,
	}
}

type OpType int

const (
	OpPut OpType = iota
	OpDel
)

// An Op is single operation that can be executed by the engine
// it is the unit that is going to be replicated accross the network
type Op struct {
	Actor ActorID
	HLC   HLCTimestamp

	Type  OpType
	Key   []byte
	Value []byte
}

// Encoding rules:
// Field order is fixed exactly as in the struct
// big endian is used for all numerical fields
// value MUST be empty if type is OpDel
// Ordering law: (hlc.wall_ns, hlc.logical, actor, op_hash)

type OpHash [32]byte

func (o *Op) Hash() OpHash {
	h := sha256.New()

	enc := o.Encode()
	h.Write(enc)

	var hash OpHash
	copy(hash[:], h.Sum(nil))
	return hash
}

type OpCannonicalKey [60]byte

// CanonicalKey returns a key formed by (HLC || actorId || opHash)
func (o *Op) CanonicalKey() OpCannonicalKey {
	var buf bytes.Buffer

	// Encode HLC timestamp (8 bytes wall_ns + 4 bytes logical, big endian)
	binary.Write(&buf, binary.BigEndian, o.HLC.wallns)
	binary.Write(&buf, binary.BigEndian, o.HLC.logical)

	// Encode Actor (16 bytes)
	buf.Write(o.Actor[:])

	// Encode OpHash (32 bytes)
	binary.Write(&buf, binary.BigEndian, o.Hash())

	var key OpCannonicalKey
	copy(key[:], buf.Bytes())

	return key
}

func (o *Op) Encode() []byte {
	var buf bytes.Buffer
	// Encode Actor (16 bytes)
	buf.Write(o.Actor[:])

	// Encode HLC timestamp (8 bytes wall_ns + 4 bytes logical, big endian)
	binary.Write(&buf, binary.BigEndian, o.HLC.wallns)
	binary.Write(&buf, binary.BigEndian, o.HLC.logical)

	// Encode OpType (4 bytes)
	binary.Write(&buf, binary.BigEndian, int32(o.Type))

	// Encode Key length and data
	binary.Write(&buf, binary.BigEndian, int32(len(o.Key)))
	if len(o.Key) > 0 {
		buf.Write(o.Key)
	}

	// Encode Value length and data
	binary.Write(&buf, binary.BigEndian, int32(len(o.Value)))
	if len(o.Value) > 0 {
		buf.Write(o.Value)
	}
	return buf.Bytes()
}

func DecodeOps(data []byte) (*Op, error) {
	o := Op{}
	buf := bytes.NewBuffer(data)
	// Decode Actor (16 bytes)
	_, err := buf.Read(o.Actor[:])
	if err != nil {
		return nil, err
	}

	// Decode HLC timestamp (8 bytes wall_ns + 4 bytes logical, big endian)
	o.HLC.wallns = binary.BigEndian.Uint64(buf.Next(8))
	o.HLC.logical = binary.BigEndian.Uint32(buf.Next(4))

	// Decode OpType (4 bytes)
	o.Type = OpType(binary.BigEndian.Uint32(buf.Next(4)))

	// Decode Key length and data
	lkey := binary.BigEndian.Uint32(buf.Next(4))
	if lkey > 0 {
		o.Key = buf.Next(int(lkey))
	}

	// Decode Value length and data
	lvalue := binary.BigEndian.Uint32(buf.Next(4))
	if lvalue > 0 {
		o.Value = buf.Next(int(lvalue))
	}

	return &o, nil
}
