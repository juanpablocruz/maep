package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
)

// Protocol message type constants.
const (
	Version      = 1
	MsgJoin      = 1
	MsgSync      = 2
	MsgSVSync    = 3
	MsgDeltaSync = 4
	MsgPing      = 5
	MsgID        = 6
	MsgAckID     = 7
)

// MessageHeader defines the binary header for messages.
type MessageHeader struct {
	Version byte
	Type    byte
	Length  uint32
}

// EncodeHeader serializes the header into bytes.
func EncodeHeader(header MessageHeader) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, header.Version); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, header.Type); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, header.Length); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeHeader deserializes a header from an io.Reader.
func DecodeHeader(r io.Reader) (MessageHeader, error) {
	var header MessageHeader
	if err := binary.Read(r, binary.BigEndian, &header.Version); err != nil {
		return header, err
	}
	if err := binary.Read(r, binary.BigEndian, &header.Type); err != nil {
		return header, err
	}
	if err := binary.Read(r, binary.BigEndian, &header.Length); err != nil {
		return header, err
	}
	return header, nil
}
