package maep

import (
	"fmt"
	"log"

	"github.com/cbergoon/merkletree"
)

type VersionTree struct {
	Tree *merkletree.MerkleTree
}

func NewVersionTree(c merkletree.Content) *VersionTree {
	var list []merkletree.Content

	list = append(list, c)

	tree, err := merkletree.NewTree(list)
	if err != nil {
		log.Fatal(err)
	}

	return &VersionTree{
		Tree: tree,
	}
}

func (v *VersionTree) Print() string {
	return v.Tree.String()
}

func (v *VersionTree) Root() []byte {
	return v.Tree.MerkleRoot()
}

func (v *VersionTree) VerifyTree() (bool, error) {
	return v.Tree.VerifyTree()
}

func (v *VersionTree) AddLeaf(leaf merkletree.Content) error {
	var list []merkletree.Content
	list = append(list, leaf)

	return v.Tree.RebuildTreeWith(list)
}

func (v *VersionTree) ShortRoot() string {
	return firstN(fmt.Sprintf("%x", v.Root()), 7)
}

func firstN(s string, n int) string {
	i := 0
	for j := range s {
		if i == n {
			return s[:j]
		}
		i++
	}
	return s
}
