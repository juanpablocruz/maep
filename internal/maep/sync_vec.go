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

func (svg *SyncVectorBlock) Max(target SyncVectorBlock) {
	cmp := hlc.Compare(svg.VectorClock, target.VectorClock)
	switch cmp {
	case 1:
		svg.VectorClock = target.VectorClock
		svg.NodeId = target.NodeId
		svg.LastVisitedHash = target.LastVisitedHash
	case -1:
		target.VectorClock = svg.VectorClock
		target.NodeId = svg.NodeId
		target.LastVisitedHash = svg.LastVisitedHash
	}
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
