package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"

	"github.com/juanpablocruz/maep/pkg/actor"
	"github.com/juanpablocruz/maep/pkg/hlc"
)

type OpType int

const (
	OpPut OpType = iota
	OpDel
)

// An Op is single operation that can be executed by the engine
// it is the unit that is going to be replicated accross the network
type Op struct {
	Actor actor.ActorID
	HLC   hlc.HLCTimestamp

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

type OpCannonicalKey [64]byte

// CanonicalKey returns a key formed by (HLC || actorId || opHash)
func (o *Op) CanonicalKey() OpCannonicalKey {
	var buf bytes.Buffer

	// Encode HLC timestamp (8 bytes wall_ns + 8 bytes logical, big endian)
	binary.Write(&buf, binary.BigEndian, o.HLC.TS)
	binary.Write(&buf, binary.BigEndian, o.HLC.Count)

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
	binary.Write(&buf, binary.BigEndian, o.HLC.TS)
	binary.Write(&buf, binary.BigEndian, o.HLC.Count)

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

	// Decode HLC timestamp (8 bytes wall_ns + 8 bytes logical, big endian)
	WallNS := binary.BigEndian.Uint64(buf.Next(8))
	Logical := binary.BigEndian.Uint64(buf.Next(8))

	o.HLC = hlc.HLCTimestamp{
		TS:    int64(WallNS),
		Count: int64(Logical),
	}

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
