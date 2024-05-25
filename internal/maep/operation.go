package maep

import (
	"crypto/sha256"
	"encoding/gob"
	"time"

	"github.com/cbergoon/merkletree"
	"github.com/google/uuid"
)

type Operation struct {
	Arguments []byte
	Id        string
	Data      []string
	Timestamp int64
}

func NewOperation(args []byte, data []string) Operation {
	return Operation{
		Arguments: args,
		Data:      data,
		Timestamp: time.Now().Unix(),
		Id:        uuid.New().String(),
	}
}

func (on Operation) CalculateHash() ([]byte, error) {
	h := sha256.New()
	e := gob.NewEncoder(h)
	err := e.Encode(on)
	if err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func (on Operation) Equals(other merkletree.Content) (bool, error) {
	otherONId := other.(Operation).Id
	return on.Id == otherONId, nil
}
