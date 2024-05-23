package maep

import "github.com/juanpablocruz/maep/internal/maep/hlc"

type SyncVectorBlock struct {
	VectorClock     *hlc.Hybrid
	NodeId          string
	LastVisitedHash string
}

type SyncVector struct {
	PrevBlock *SyncVectorBlock
	NextBlock *SyncVectorBlock
}

func (svg *SyncVectorBlock) Max(target SyncVectorBlock) error {
	return nil
}

func NewSyncVectorBlock() *SyncVectorBlock {
	return &SyncVectorBlock{
		VectorClock: hlc.NewNow(0),
	}
}

func NewSyncVector() *SyncVector {
	return &SyncVector{
		PrevBlock: NewSyncVectorBlock(),
		NextBlock: NewSyncVectorBlock(),
	}
}
