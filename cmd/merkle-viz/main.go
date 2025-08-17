package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/juanpablocruz/maep/pkg/engine"
	"github.com/juanpablocruz/maep/pkg/merkle"
)

type TestHasher struct {
	ops   []engine.OpLogEntry
	opLog *engine.OpLog
}

func (th TestHasher) Sort(hs []merkle.Hash) {
	newOrder := make([]merkle.Hash, 0)
	for _, o := range th.opLog.GetOrdered() {
		h := merkle.Hash{}
		oH := o.CanonicalKey()
		copy(h[:], oH[:])
		newOrder = append(newOrder, h)
	}
	copy(hs, newOrder)
}

func NewTestHasher() *TestHasher {
	ops := engine.GenerateOps(10)
	opLog := engine.NewOpLog()
	for _, o := range ops {
		opLog.Append(&o)
	}
	allOpLogs := make([]engine.OpLogEntry, 0)
	for _, entry := range opLog.GetAll() {
		allOpLogs = append(allOpLogs, *entry)
	}

	th := &TestHasher{
		ops:   allOpLogs,
		opLog: opLog,
	}
	return th
}

type OpLogMerkleEntry struct {
	opLog engine.OpLogEntry
	hash  engine.OpCannonicalKey
}

func (ome OpLogMerkleEntry) ComputeHash() merkle.Hash {
	return merkle.Hash(ome.hash[:])
}

func main() {
	fmt.Println("Merkle Tree Visualizer")
	fmt.Println("======================")
	fmt.Println()

	// Create a test hasher
	th := NewTestHasher()

	// Create Merkle tree
	cfg := merkle.Config{
		Fanout:   16,
		MaxDepth: 6,
		Hasher:   th,
	}

	m, err := merkle.New(cfg)
	if err != nil {
		fmt.Printf("Error creating merkle tree: %v\n", err)
		os.Exit(1)
	}

	// Add some initial operations
	fmt.Println("Adding initial operations...")
	for i := 0; i < 5; i++ {
		ome := OpLogMerkleEntry{}
		ome.opLog = th.ops[i]
		ome.hash = ome.opLog.CanonicalKey()

		err = m.AppendOp(ome)
		if err != nil {
			fmt.Printf("Error appending operation %d: %v\n", i, err)
			os.Exit(1)
		}
	}

	// Create visualizer
	viz := merkle.NewVisualizer(m)

	// Interactive loop
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("\nCommands:")
		fmt.Println("  tree    - Show tree visualization")
		fmt.Println("  stats   - Show tree statistics")
		fmt.Println("  path <n> - Show path to operation n (0-9)")
		fmt.Println("  add <n>  - Add operation n to tree")
		fmt.Println("  quit     - Exit")
		fmt.Print("\nEnter command: ")

		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		parts := strings.Fields(input)
		if len(parts) == 0 {
			continue
		}

		command := parts[0]

		switch command {
		case "tree":
			fmt.Println("\n" + viz.VisualizeTree())

		case "stats":
			fmt.Println("\n" + viz.GetTreeStats())

		case "path":
			if len(parts) < 2 {
				fmt.Println("Usage: path <n>")
				continue
			}
			n, err := strconv.Atoi(parts[1])
			if err != nil || n < 0 || n >= len(th.ops) {
				fmt.Printf("Invalid operation number. Must be 0-%d\n", len(th.ops)-1)
				continue
			}
			ome := OpLogMerkleEntry{
				opLog: th.ops[n],
				hash:  th.ops[n].CanonicalKey(),
			}
			fmt.Println("\n" + viz.VisualizePath(ome.ComputeHash()))

		case "add":
			if len(parts) < 2 {
				fmt.Println("Usage: add <n>")
				continue
			}
			n, err := strconv.Atoi(parts[1])
			if err != nil || n < 0 || n >= len(th.ops) {
				fmt.Printf("Invalid operation number. Must be 0-%d\n", len(th.ops)-1)
				continue
			}
			ome := OpLogMerkleEntry{
				opLog: th.ops[n],
				hash:  th.ops[n].CanonicalKey(),
			}
			err = m.AppendOp(ome)
			if err != nil {
				fmt.Printf("Error adding operation: %v\n", err)
			} else {
				fmt.Printf("Added operation %d to tree\n", n)
			}

		case "quit", "exit":
			fmt.Println("Goodbye!")
			return

		default:
			fmt.Printf("Unknown command: %s\n", command)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %v\n", err)
	}
}
