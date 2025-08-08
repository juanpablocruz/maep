package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"slices"

	"github.com/juanpablocruz/maep/pkg/hlc"
	"github.com/juanpablocruz/maep/pkg/materialize"
	"github.com/juanpablocruz/maep/pkg/merkle"
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/oplog"
)

func main() {
	l := oplog.New()
	clk := hlc.New()

	var actor model.ActorID
	copy(actor[:], bytes.Repeat([]byte{0xAB}, 16))

	put := func(key, val string) {
		op := model.Op{
			Version:  model.OpSchemaV1,
			Kind:     model.OpKindPut,
			Key:      key,
			Value:    []byte(val),
			HLCTicks: clk.Now(),
			Actor:    actor,
		}
		op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor)
		l.Append(op)
	}
	del := func(key string) {
		op := model.Op{
			Version:  model.OpSchemaV1,
			Kind:     model.OpKindDel,
			Key:      key,
			HLCTicks: clk.Now(),
			Actor:    actor,
		}
		op.Hash = model.HashOp(op.Version, op.Kind, op.Key, op.Value, op.HLCTicks, op.WallNanos, op.Actor)
		l.Append(op)
	}

	// Demo sequence
	put("A", "v1")
	put("B", "v2")
	put("A", "v3") // overwrite A
	del("B")       // delete B

	// --- Materialize + Merkle ---
	view := materialize.Snapshot(l)
	leaves := materialize.LeavesFromSnapshot(view)
	root := merkle.Build(leaves)

	// --- Print table ---
	fmt.Println("Current state (LWW):")
	printTable(view)

	// --- Print leaves & root ---
	fmt.Println()
	fmt.Println("Merkle leaves:")
	printLeaves(leaves)
	fmt.Printf("\nMerkle root: %s\n", hexShort(root[:], 12))

	// --- ASCII Merkle tree (small, for visual sanity) ---
	fmt.Println("\nASCII Merkle tree:")
	printAsciiMerkle(leaves)
}

func printTable(view map[string]materialize.State) {
	// Stable key order
	keys := make([]string, 0, len(view))
	for k := range view {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	fmt.Printf("%-8s %-8s %-14s %-14s %-12s %-12s\n", "Key", "Present", "Value", "LastKind", "HLCTicks", "Actor")
	fmt.Printf("%s\n", strings.Repeat("-", 70))
	for _, k := range keys {
		st := view[k]
		val := ""
		if st.Present && len(st.Value) > 0 {
			val = string(st.Value)
			if len(val) > 12 {
				val = val[:12] + "…"
			}
		}
		kind := kindName(st.Last.Kind)
		fmt.Printf("%-8s %-8v %-14s %-14s %-12d %-12s\n",
			k, st.Present, val, kind, st.Last.HLCTicks, hexShort(st.Last.Actor[:], 6))
	}
}

func printLeaves(ls []merkle.Leaf) {
	sort.Slice(ls, func(i, j int) bool { return ls[i].Key < ls[j].Key })
	for _, lf := range ls {
		fmt.Printf("  %-8s %s\n", lf.Key, hexShort(lf.Hash[:], 16))
	}
}

func printAsciiMerkle(ls []merkle.Leaf) {
	if len(ls) == 0 {
		fmt.Println("(empty)")
		return
	}
	// Sort leaves by key for deterministic order
	sorted := slices.Clone(ls)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })

	// Level 0: leaves
	level := make([][]byte, len(sorted))
	for i, lf := range sorted {
		level[i] = slices.Clone(lf.Hash[:])
	}
	// Print leaves
	fmt.Printf("L0: ")
	for i := range level {
		fmt.Printf("%s ", hexShort(level[i], 8))
	}
	fmt.Println()

	// Build up
	L := 1
	for n := len(level); n > 1; L++ {
		next := make([][]byte, (n+1)/2)
		for i := 0; i < n; i += 2 {
			if i+1 < n {
				h := sha256.New()
				h.Write(level[i])
				h.Write(level[i+1])
				next[i/2] = h.Sum(nil)
			} else {
				next[i/2] = level[i]
			}
		}
		fmt.Printf("L%d: ", L)
		for i := range next {
			fmt.Printf("%s ", hexShort(next[i], 8))
		}
		fmt.Println()
		level = next
		n = len(level)
	}
}

func hexShort(b []byte, n int) string {
	if n*2 > len(b)*2 {
		n = len(b)
	}
	s := hex.EncodeToString(b)
	if len(s) > n {
		s = s[:n]
	}
	return s
}

func kindName(k uint8) string {
	switch k {
	case model.OpKindPut:
		return "Put"
	case model.OpKindDel:
		return "Delete"
	default:
		return fmt.Sprintf("Kind(%d)", k)
	}
}
