package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type eventLine struct {
	Kind   string                 `json:"kind"`
	Time   time.Time              `json:"time"`
	Node   string                 `json:"node"`
	Type   string                 `json:"type"`
	Fields map[string]interface{} `json:"fields"`
}

type snapshot struct {
	Name      string `json:"name"`
	Port      int    `json:"port"`
	Connected bool   `json:"connected"`
	Paused    bool   `json:"paused"`
	Keys      int    `json:"keys"`
	Root      string `json:"root"`
}

type commandLine struct {
	Kind   string     `json:"kind"`
	Time   time.Time  `json:"time"`
	Cmd    string     `json:"cmd"`
	Before []snapshot `json:"before"`
	After  []snapshot `json:"after"`
}

func main() {
	dir := flag.String("dir", "logs", "directory with *.jsonl logs")
	flag.Parse()

	events, _ := readLines[*eventLine](*dir, "events-*.jsonl")
	wire, _ := readLines[*eventLine](*dir, "wire-*.jsonl")
	cmds, _ := readLines[*commandLine](*dir, "commands-*.jsonl")

	printHeader("LOG REPORT")
	fmt.Printf("Directory: %s\n\n", *dir)

	printEvents(events)
	printWire(wire)
	printCommands(cmds)
}

func readLines[T any](dir, pattern string) ([]T, error) {
	glob := filepath.Join(dir, pattern)
	files, err := filepath.Glob(glob)
	if err != nil || len(files) == 0 {
		return nil, err
	}
	// Pick the latest file per pattern
	sort.Strings(files)
	last := files[len(files)-1]
	f, err := os.Open(last)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	var out []T
	for s.Scan() {
		line := s.Bytes()
		var v T
		if err := json.Unmarshal(line, &v); err == nil {
			out = append(out, v)
		}
	}
	_ = s.Err()
	return out, nil
}

func printHeader(title string) {
	fmt.Println(strings.Repeat("=", len(title)))
	fmt.Println(title)
	fmt.Println(strings.Repeat("=", len(title)))
}

func printEvents(events []*eventLine) {
	fmt.Println("Events Summary")
	if len(events) == 0 {
		fmt.Println("  (no events)")
		fmt.Println()
		return
	}

	byType := map[string]int{}
	byNode := map[string]int{}
	// sync session durations per node
	sessStart := map[string]map[string]time.Time{}
	var sessDur []time.Duration

	for _, e := range events {
		byType[e.Type]++
		byNode[e.Node]++
		if e.Type == "sync" {
			act, _ := e.Fields["action"].(string)
			id := fmt.Sprint(e.Fields["id"])
			switch act {
			case "begin":
				if sessStart[e.Node] == nil {
					sessStart[e.Node] = map[string]time.Time{}
				}
				sessStart[e.Node][id] = e.Time
			case "end":
				if t0, ok := sessStart[e.Node][id]; ok {
					sessDur = append(sessDur, e.Time.Sub(t0))
					delete(sessStart[e.Node], id)
				}
			}
		}
	}

	fmt.Println("  By type:")
	for _, k := range sortedKeys(byType) {
		fmt.Printf("    %-16s %6d\n", k, byType[k])
	}
	fmt.Println("  By node:")
	for _, k := range sortedKeys(byNode) {
		fmt.Printf("    %-8s %6d\n", k, byNode[k])
	}
	if len(sessDur) > 0 {
		total := time.Duration(0)
		for _, d := range sessDur {
			total += d
		}
		avg := total / time.Duration(len(sessDur))
		fmt.Printf("  Sync sessions: %d  avg=%s\n", len(sessDur), avg)
	}
	fmt.Println()
}

func printWire(wire []*eventLine) {
	fmt.Println("Wire Summary")
	if len(wire) == 0 {
		fmt.Println("  (no wire logs)")
		fmt.Println()
		return
	}
	byKey := map[string]struct{ Count, Bytes int }{}
	for _, e := range wire {
		proto := fmt.Sprint(e.Fields["proto"]) // tcp/udp
		dir := fmt.Sprint(e.Fields["dir"])     // <- or ->
		mt := fmt.Sprint(e.Fields["mt"])       // numeric
		name := mt
		if n, ok := e.Fields["name"]; ok {
			name = fmt.Sprint(n)
		}
		b := 0
		if v, ok := e.Fields["bytes"].(float64); ok {
			b = int(v)
		}
		key := fmt.Sprintf("%s %2s %-12s", proto, dir, name)
		agg := byKey[key]
		agg.Count++
		agg.Bytes += b
		byKey[key] = agg
	}
	for _, k := range sortedKeysStruct(byKey) {
		agg := byKey[k]
		fmt.Printf("  %-20s  count=%5d  bytes=%7d\n", k, agg.Count, agg.Bytes)
	}
	fmt.Println()
}

func printCommands(cmds []*commandLine) {
	fmt.Println("Commands Summary")
	if len(cmds) == 0 {
		fmt.Println("  (no commands)")
		fmt.Println()
		return
	}
	for _, c := range cmds {
		fmt.Printf("  %s  %s\n", c.Time.Format(time.RFC3339), c.Cmd)
		if len(c.Before) > 0 && len(c.After) > 0 {
			// Show per-node deltas
			byName := map[string]snapshot{}
			for _, s := range c.Before {
				byName[s.Name] = s
			}
			for _, s := range c.After {
				b := byName[s.Name]
				rootChanged := b.Root != s.Root
				keysDelta := s.Keys - b.Keys
				linkChange := stateChange(b.Connected, s.Connected)
				pauseChange := stateChange(b.Paused, s.Paused)
				if rootChanged || keysDelta != 0 || linkChange != "" || pauseChange != "" {
					fmt.Printf("    %-8s keys %+d rootChanged=%v %s %s\n", s.Name, keysDelta, rootChanged, linkChange, pauseChange)
				}
			}
		}
	}
	fmt.Println()
}

func stateChange(a, b bool) string {
	if a == b {
		return ""
	}
	if b {
		return "-> on"
	}
	return "-> off"
}

func sortedKeys(m map[string]int) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func sortedKeysStruct(m map[string]struct{ Count, Bytes int }) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
