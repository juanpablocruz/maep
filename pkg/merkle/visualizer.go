package merkle

import (
	"fmt"
	"strings"
)

// Visualizer provides methods to visualize the Merkle tree structure
type Visualizer struct {
	tree *Merkle
}

// NewVisualizer creates a new visualizer for the given Merkle tree
func NewVisualizer(tree *Merkle) *Visualizer {
	return &Visualizer{tree: tree}
}

// VisualizeTree generates an ASCII representation of the entire tree
func (v *Visualizer) VisualizeTree() string {
	var sb strings.Builder
	sb.WriteString("Merkle Tree Visualization\n")
	sb.WriteString("========================\n\n")

	v.tree.mu.RLock()
	defer v.tree.mu.RUnlock()

	sb.WriteString(fmt.Sprintf("Fanout: %d, MaxDepth: %d\n\n", v.tree.fanout, v.tree.maxDepth))

	if v.tree.root == nil {
		sb.WriteString("Empty tree\n")
		return sb.String()
	}

	v.visualizeNode(v.tree.root, "", true, &sb)
	return sb.String()
}

// visualizeNode recursively visualizes a node and its children
func (v *Visualizer) visualizeNode(node *MerkleNode, prefix string, isLast bool, sb *strings.Builder) {
	// Node representation
	connector := "├── "
	if isLast {
		connector = "└── "
	}

	sb.WriteString(prefix + connector)

	// Node info
	if node == nil {
		sb.WriteString("nil\n")
		return
	}

	// Show hash (first 8 bytes for readability)
	hashStr := fmt.Sprintf("%x", node.Hash[:8])
	if node.Hash == zeroHash() {
		hashStr = "00000000"
	}

	sb.WriteString(fmt.Sprintf("Hash: %s, Count: %d", hashStr, node.Count))

	// Show LastK if not zero
	if node.LastK != zeroHash() {
		lastKStr := fmt.Sprintf("%x", node.LastK[:8])
		sb.WriteString(fmt.Sprintf(", LastK: %s", lastKStr))
	}

	// Show number of operations if this is a leaf
	if node.Ops != nil && len(node.Ops) > 0 {
		sb.WriteString(fmt.Sprintf(", Ops: %d", len(node.Ops)))
	}

	sb.WriteString("\n")

	// Recursively visualize children
	childPrefix := prefix
	if isLast {
		childPrefix += "    "
	} else {
		childPrefix += "│   "
	}

	// Find non-nil children
	nonNilChildren := 0
	for i := 0; i < 16; i++ {
		if node.Child[i] != nil {
			nonNilChildren++
		}
	}

	if nonNilChildren == 0 {
		return
	}

	childCount := 0
	for i := 0; i < 16; i++ {
		if node.Child[i] != nil {
			childCount++
			isLastChild := childCount == nonNilChildren
			sb.WriteString(fmt.Sprintf("%s[%d] ", childPrefix, i))
			v.visualizeNode(node.Child[i], childPrefix, isLastChild, sb)
		}
	}
}

// VisualizePath visualizes the path from root to a specific hash
func (v *Visualizer) VisualizePath(targetHash Hash) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Path to Hash: %x\n", targetHash[:8]))
	sb.WriteString("=====================\n\n")

	v.tree.mu.RLock()
	defer v.tree.mu.RUnlock()

	digits, err := KeyDigits16(targetHash, v.tree.maxDepth)
	if err != nil {
		sb.WriteString(fmt.Sprintf("Error computing path: %v\n", err))
		return sb.String()
	}

	sb.WriteString("Path digits: ")
	for i, digit := range digits {
		if i > 0 {
			sb.WriteString(" -> ")
		}
		sb.WriteString(fmt.Sprintf("%d", digit))
	}
	sb.WriteString("\n\n")

	// Traverse the path
	node := v.tree.root
	path := []*MerkleNode{node}

	for i, idx := range digits {
		sb.WriteString(fmt.Sprintf("Level %d (digit %d):\n", i, idx))

		// Show current node info
		hashStr := fmt.Sprintf("%x", node.Hash[:8])
		if node.Hash == zeroHash() {
			hashStr = "00000000"
		}
		sb.WriteString(fmt.Sprintf("  Node Hash: %s, Count: %d\n", hashStr, node.Count))

		// Show children at this level
		sb.WriteString("  Children: ")
		for j := 0; j < 16; j++ {
			if node.Child[j] != nil {
				childHash := fmt.Sprintf("%x", node.Child[j].Hash[:8])
				if node.Child[j].Hash == zeroHash() {
					childHash = "00000000"
				}
				sb.WriteString(fmt.Sprintf("[%d:%s]", j, childHash))
			}
		}
		sb.WriteString("\n")

		// Move to next level
		if node.Child[idx] == nil {
			sb.WriteString(fmt.Sprintf("  -> Child[%d] is nil (path ends here)\n", idx))
			break
		}

		node = node.Child[idx]
		path = append(path, node)
		sb.WriteString(fmt.Sprintf("  -> Following child[%d]\n\n", idx))
	}

	// Show final leaf node if we reached it
	if len(path) > 1 {
		leaf := path[len(path)-1]
		sb.WriteString("Final leaf node:\n")
		hashStr := fmt.Sprintf("%x", leaf.Hash[:8])
		if leaf.Hash == zeroHash() {
			hashStr = "00000000"
		}
		sb.WriteString(fmt.Sprintf("  Hash: %s, Count: %d\n", hashStr, leaf.Count))

		if leaf.Ops != nil {
			sb.WriteString(fmt.Sprintf("  Operations: %d\n", len(leaf.Ops)))
			for opHash := range leaf.Ops {
				opHashStr := fmt.Sprintf("%x", opHash[:8])
				sb.WriteString(fmt.Sprintf("    - %s\n", opHashStr))
			}
		}
	}

	return sb.String()
}

// GetTreeStats returns detailed statistics about the tree
func (v *Visualizer) GetTreeStats() string {
	var sb strings.Builder
	sb.WriteString("Merkle Tree Statistics\n")
	sb.WriteString("======================\n\n")

	v.tree.mu.RLock()
	defer v.tree.mu.RUnlock()

	stats := v.computeStats(v.tree.root, 0)

	sb.WriteString(fmt.Sprintf("Total nodes: %d\n", stats.totalNodes))
	sb.WriteString(fmt.Sprintf("Leaf nodes: %d\n", stats.leafNodes))
	sb.WriteString(fmt.Sprintf("Internal nodes: %d\n", stats.internalNodes))
	sb.WriteString(fmt.Sprintf("Max depth reached: %d\n", stats.maxDepth))
	sb.WriteString(fmt.Sprintf("Total operations: %d\n", stats.totalOps))
	sb.WriteString(fmt.Sprintf("Average operations per leaf: %.2f\n", stats.avgOpsPerLeaf))
	sb.WriteString(fmt.Sprintf("Tree height: %d\n", stats.treeHeight))

	return sb.String()
}

type treeStats struct {
	totalNodes    int
	leafNodes     int
	internalNodes int
	maxDepth      int
	totalOps      int
	avgOpsPerLeaf float64
	treeHeight    int
}

func (v *Visualizer) computeStats(node *MerkleNode, depth int) treeStats {
	if node == nil {
		return treeStats{}
	}

	stats := treeStats{
		totalNodes: 1,
		maxDepth:   depth,
	}

	// Count operations in this node
	if node.Ops != nil {
		stats.totalOps = len(node.Ops)
		stats.leafNodes = 1
	} else {
		stats.internalNodes = 1
	}

	// Recursively compute stats for children
	hasChildren := false
	for i := 0; i < 16; i++ {
		if node.Child[i] != nil {
			hasChildren = true
			childStats := v.computeStats(node.Child[i], depth+1)
			stats.totalNodes += childStats.totalNodes
			stats.leafNodes += childStats.leafNodes
			stats.internalNodes += childStats.internalNodes
			stats.totalOps += childStats.totalOps
			if childStats.maxDepth > stats.maxDepth {
				stats.maxDepth = childStats.maxDepth
			}
		}
	}

	// Update tree height
	if !hasChildren && depth > stats.treeHeight {
		stats.treeHeight = depth
	}

	// Calculate average operations per leaf
	if stats.leafNodes > 0 {
		stats.avgOpsPerLeaf = float64(stats.totalOps) / float64(stats.leafNodes)
	}

	return stats
}
