package maep

import (
	"log"

	"github.com/cbergoon/merkletree"
)

type VersionTree struct {
	tree *merkletree.MerkleTree
}

func NewVersionTree(c merkletree.Content) *VersionTree {
	var list []merkletree.Content

	list = append(list, c)

	tree, err := merkletree.NewTree(list)
	if err != nil {
		log.Fatal(err)
	}

	return &VersionTree{
		tree: tree,
	}
}

func (v *VersionTree) Print() string {
	return v.tree.String()
}

func (v *VersionTree) Root() []byte {
	return v.tree.MerkleRoot()
}

func (v *VersionTree) VerifyTree() (bool, error) {
	return v.tree.VerifyTree()
}

func (v *VersionTree) AddLeaf(leaf merkletree.Content) error {
	var list []merkletree.Content
	list = append(list, leaf)

	return v.tree.RebuildTreeWith(list)
}
