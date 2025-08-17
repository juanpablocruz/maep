package merkle

type MerkleSnapshot struct {
	fanout   int
	maxDepth int
	root     Hash
	epoch    uint64
}

func (m MerkleSnapshot) Fanout() int   { return m.fanout }
func (m MerkleSnapshot) MaxDepth() int { return m.maxDepth }
func (m MerkleSnapshot) Root() Hash    { return m.root }
func (m MerkleSnapshot) Children(p Prefix) ([]Summary, error) {
	return nil, nil
}
func (m MerkleSnapshot) ProofForKey(key Hash) (Proof, error) { return Proof{}, nil }
func (m MerkleSnapshot) Epoch() uint64                       { return m.epoch }
func (m *MerkleSnapshot) Close()                             {}
