package actor

import (
	"github.com/google/uuid"
)

// An ActorID is a unique identifier for a node,
// it is assigned at node creation and never changes
type ActorID [16]byte

// PeerID is an alias for ActorID used in networking contexts
type PeerID = ActorID

func NewActor() ActorID {
	uid := uuid.New()

	actorId := ActorID{}
	copy(actorId[:], uid[:])
	return actorId
}
