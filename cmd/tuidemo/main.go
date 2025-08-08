package main

import (
	"bytes"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"golang.org/x/term"

	"slices"

	"github.com/juanpablocruz/maep/pkg/materialize"
	"github.com/juanpablocruz/maep/pkg/merkle"
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/node"
	"github.com/juanpablocruz/maep/pkg/segment"
	"github.com/juanpablocruz/maep/pkg/syncproto"
	"github.com/juanpablocruz/maep/pkg/transport"
)

// ---------- layout & widths ----------
const (
	headerRowsTop = 4 // header + status lines
	panelHeaderH  = 3 // title + 2 header lines in each panel
	maxEvents     = 30
)

func calcLayout() (screenW, rightPanelX, leftW, rightW int) {
	w, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || w <= 0 {
		w = 200
	}
	screenW = w
	rightPanelX = w / 2
	leftW = rightPanelX - 2
	rightW = w - rightPanelX - 2
	return
}

// ---------- ANSI colors ----------
const (
	clrReset   = "\x1b[0m"
	clrBold    = "\x1b[1m"
	clrDim     = "\x1b[2m"
	clrRed     = "\x1b[31m"
	clrGreen   = "\x1b[32m"
	clrYellow  = "\x1b[33m"
	clrBlue    = "\x1b[34m"
	clrMagenta = "\x1b[35m"
	clrCyan    = "\x1b[36m"
	clrGray    = "\x1b[90m"
)

func main() {
	// Transport & nodes
	sw := transport.NewSwitch()
	epA, _ := sw.Listen("A")
	epB, _ := sw.Listen("B")
	defer epA.Close()
	defer epB.Close()

	nA := node.New("A", epA, "B", 500*time.Millisecond)
	nB := node.New("B", epB, "A", 500*time.Millisecond)

	// Events
	evCh := make(chan node.Event, 256)
	nA.AttachEvents(evCh)
	nB.AttachEvents(evCh)

	nA.Start()
	nB.Start()
	defer nA.Stop()
	defer nB.Stop()

	// Actors
	var actA, actB model.ActorID
	copy(actA[:], bytes.Repeat([]byte{0xA1}, 16))
	copy(actB[:], bytes.Repeat([]byte{0xB2}, 16))

	// Seed ops on A
	nA.Put("A", []byte("v1"), actA)
	nA.Put("B", []byte("v2"), actA)
	nA.Delete("B", actA)
	nA.Put("C", []byte("v3"), actA)

	// Raw terminal
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		panic(err)
	}

	defer term.Restore(int(os.Stdin.Fd()), oldState)
	installUI()
	defer restoreUI()

	events := make([]node.Event, 0, 128)
	input := "" // live command buffer

	// key/command reader
	inputEditCh := make(chan string, 64) // live edits
	cmdCh := make(chan string, 16)       // completed lines
	quitCh := make(chan struct{})
	go readKeys(inputEditCh, cmdCh, quitCh)

	// exit on Ctrl-C too (raw reader also exits on ^C)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	// repaint timer
	tick := time.NewTicker(120 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case e := <-evCh:
			if e.Type == node.EventHB {
				// Optionally: keep a counter/timestamp if you want to show a tiny pulse elsewhere.
				break
			}
			events = append(events, e)
			if len(events) > maxEvents {
				events = events[len(events)-maxEvents:]
			}
		case <-tick.C:
			render(nA, nB, events, input)
		case s := <-inputEditCh:
			input = s
			render(nA, nB, events, input) // immediate prompt redraw
		case line := <-cmdCh:
			input = ""
			if handleCmd(strings.TrimSpace(line), nA, nB, actA, actB, &events) {
				return
			}
		case <-quitCh:
			return
		case <-sig:
			return
		}
	}
}

// -------- Commands --------

func handleCmd(line string, nA, nB *node.Node, actA, actB model.ActorID, events *[]node.Event) bool {
	if line == "" {
		return false
	}
	parts := strings.Fields(line)
	cmd := strings.ToLower(parts[0])

	emit := func(s string) {
		*events = append(*events, node.Event{
			Time: time.Now(), Node: "UI", Type: node.EventType("cmd"),
			Fields: map[string]any{"cmd": s},
		})
	}

	switch cmd {
	case "q", "quit", "exit":
		return true
	case "help", "h", "?":
		emit("help")
	case "wa", "wb":
		if len(parts) < 3 {
			emit("usage: wa|wb <key> <val>")
			return false
		}
		key, val := parts[1], strings.Join(parts[2:], " ")
		if cmd == "wa" {
			nA.Put(key, []byte(val), actA)
		} else {
			nB.Put(key, []byte(val), actB)
		}
	case "da", "db":
		if len(parts) < 2 {
			emit("usage: da|db <key>")
			return false
		}
		key := parts[1]
		if cmd == "da" {
			nA.Delete(key, actA)
		} else {
			nB.Delete(key, actB)
		}
	case "rand":
		target := "a"
		if len(parts) >= 2 {
			target = strings.ToLower(parts[1])
		}
		k := randKey()
		v := fmt.Sprintf("v%d", time.Now().UnixNano()%1000)
		if target == "b" {
			nB.Put(k, []byte(v), actB)
		} else {
			nA.Put(k, []byte(v), actA)
		}
	case "link":
		if len(parts) < 3 {
			emit("usage: link <a|b> <up|down>")
			return false
		}
		nodeSel := strings.ToLower(parts[1])
		up := parts[2] == "up"
		switch nodeSel {
		case "a":
			nA.SetConnected(up)
		case "b":
			nB.SetConnected(up)
		default:
			emit("usage: link <a|b> <up|down>")
		}
	case "pause":
		if len(parts) < 3 {
			emit("usage: pause <a|b> <on|off>")
			return false
		}
		nodeSel := strings.ToLower(parts[1])
		on := parts[2] == "on"
		switch nodeSel {
		case "a":
			nA.SetPaused(on)
		case "b":
			nB.SetPaused(on)
		default:
			emit("usage: pause <a|b> <on|off>")
		}
	case "burst":
		if len(parts) < 3 {
			emit("usage: burst <a|b> <n>")
			return false
		}
		nodeSel := strings.ToLower(parts[1])
		n := atoiSafe(parts[2])
		for i := range n {
			k := randKey()
			v := fmt.Sprintf("v%d", time.Now().UnixNano()%(1000+int64(i)))
			if nodeSel == "b" {
				nB.Put(k, []byte(v), actB)
			} else {
				nA.Put(k, []byte(v), actA)
			}
		}
	default:
		emit("unknown: " + line)
	}
	return false
}

// Raw key reader (single-keystroke). Shows live prompt and emits full lines.
func readKeys(editOut chan<- string, lineOut chan<- string, quit chan<- struct{}) {
	buf := make([]byte, 1)
	cur := []rune{}
	for {
		if _, err := os.Stdin.Read(buf); err != nil {
			continue
		}
		b := buf[0]
		switch b {
		case 3: // Ctrl-C
			close(quit)
			return
		case '\r', '\n':
			lineOut <- string(cur)
			cur = cur[:0]
			editOut <- ""
		case 127, 8: // Backspace / Ctrl-H
			if len(cur) > 0 {
				cur = cur[:len(cur)-1]
				editOut <- string(cur)
			}
		case 27: // swallow ESC [ X (arrow keys)
			rest := make([]byte, 2)
			_, _ = os.Stdin.Read(rest)
		default:
			if b >= 32 && b <= 126 {
				cur = append(cur, rune(b))
				editOut <- string(cur)
			}
		}
	}
}

// -------- Rendering --------

func render(nA, nB *node.Node, events []node.Event, input string) {
	clearScreen()

	screenW, rightPanelX, leftW, rightW := calcLayout()
	// Snapshots + roots
	viewA := materialize.Snapshot(nA.Log)
	viewB := materialize.Snapshot(nB.Log)
	leavesA := materialize.LeavesFromSnapshot(viewA)
	leavesB := materialize.LeavesFromSnapshot(viewB)
	rootA := merkle.Build(leavesA)
	rootB := merkle.Build(leavesB)
	segA := segment.RootsBySegment(viewA)
	segB := segment.RootsBySegment(viewB)
	segDiff := diffSegments(segA, segB)

	diff := diffKeys(leavesA, leavesB)
	inSync := rootA == rootB

	// Header (full-line clears)
	printFull(0, fmt.Sprintf(" %sMAEP Sync TUI%s — %s  (type '%shelp%s' for commands; Enter to run)",
		clrBold, clrReset, time.Now().Format("15:04:05"), clrCyan, clrReset))
	printFull(1, stringsRepeat("─", screenW))

	status := colorize("IN SYNC", clrGreen)
	if !inSync {
		status = colorize("OUT OF SYNC", clrRed)
	}
	printFull(2, fmt.Sprintf(" %s   differ: %s  %s   segs differ: %s  %s",
		status,
		colorize(fmt.Sprintf("%d", len(diff)), clrYellow), truncateList(diff, 8),
		colorize(fmt.Sprintf("%d", len(segDiff)), clrYellow), truncateSegList(segDiff, 8)))
	printFull(3, stringsRepeat("─", screenW))

	// Panels + flags (box prints within columns)
	row := headerRowsTop
	drawPanel(0, row, leftW,
		fmt.Sprintf("%sNode A%s [link:%s pause:%s health:%s]",
			clrCyan, clrReset, onOff(nA.IsConnected()), onOff(nA.IsPaused()), healthLabel(nA)),
		viewA, rootA[:])

	drawPanel(rightPanelX, row, rightW,
		fmt.Sprintf("%sNode B%s [link:%s pause:%s health:%s]",
			clrCyan, clrReset, onOff(nB.IsConnected()), onOff(nB.IsPaused()), healthLabel(nB)),
		viewB, rootB[:])

	// Compute space used by panels to place events area
	rowsA := panelRows(viewA)
	rowsB := panelRows(viewB)
	usedRows := panelHeaderH + maxInt(rowsA, rowsB)

	evStart := row + usedRows + 1
	printFull(evStart, stringsRepeat("─", screenW))
	printFull(evStart+1, " Events (latest first):")

	lines := formatEvents(events)
	// Normalize to maxEvents lines and clear stale content
	for i := range maxEvents {
		y := evStart + 2 + i
		if i < len(lines) {
			printFull(y, "  "+lines[len(lines)-1-i]) // newest first
		} else {
			printFull(y, "")
		}
	}

	// Footer: commands & prompt
	footerY := evStart + 2 + maxEvents + 1
	printFull(footerY, stringsRepeat("─", screenW))
	printFull(footerY+1, fmt.Sprintf(" %sCommands%s: %swa|wb%s <key> <val> | %sda|db%s <key> | %srand%s [a|b] | %slink%s <a|b> <up|down> | %spause%s <a|b> <on|off> | %sburst%s <a|b> <n> | %sq%s",
		clrBold, clrReset,
		clrYellow, clrReset,
		clrYellow, clrReset,
		clrYellow, clrReset,
		clrYellow, clrReset,
		clrYellow, clrReset,
		clrYellow, clrReset,
		clrYellow, clrReset))
	printFull(footerY+2, fmt.Sprintf(" %scmd>%s %s", clrBlue, clrReset, input))
}

// panelRows returns number of data rows for a panel
func panelRows(view map[string]materialize.State) int { return len(view) }

// drawPanel prints a panel strictly inside its column using printBox
func drawPanel(x, y, w int, title string, view map[string]materialize.State, root []byte) {

	printBox(x, y+0, w, fmt.Sprintf("[%s]  root: %s", title, shortHex(root, 12)))
	printBox(x, y+1, w, " Key     Present  Value")
	printBox(x, y+2, w, stringsRepeat("-", minInt(w, 36)))

	keys := make([]string, 0, len(view))
	for k := range view {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	row := 0
	for _, k := range keys {
		st := view[k]
		val := ""
		if st.Present {
			val = string(st.Value)
		}
		present := colorize(fmt.Sprintf("%-7v", st.Present), ifThen(st.Present, clrGreen, clrRed))
		printBox(x, y+3+row, w, fmt.Sprintf(" %-7s %s  %s", k, present, colorize(truncate(val, 16), clrBlue)))
		row++
	}
	// clear a couple extra lines within the panel box to avoid stale rows
	for i := range 2 {
		printBox(x, y+3+row+i, w, "")
	}
}

// ----- Event formatting -----

func formatEvents(ev []node.Event) []string {
	out := make([]string, 0, len(ev))
	for _, e := range ev {
		switch e.Type {
		case node.EventSync:
			act, _ := e.Fields["action"].(string)
			id := e.Fields["id"]
			col := clrMagenta
			if act == "end" {
				col = clrCyan
			}
			out = append(out, fmt.Sprintf("%s%-6s%s %s SYNC %s id=%v",
				clrGray, e.Node, clrReset, col, act, id))
		case node.EventPut:
			out = append(out, fmt.Sprintf("%s%-6s%s %s PUT  %s=%s %s=%s %s=%v",
				clrGray, e.Node, clrReset, clrGreen, "key", e.Fields["key"], "val", e.Fields["val"], "hlc", e.Fields["hlc"]))
		case node.EventDel:
			out = append(out, fmt.Sprintf("%s%-6s%s %s DEL  %s=%s %s=%v",
				clrGray, e.Node, clrReset, clrRed, "key", e.Fields["key"], "hlc", e.Fields["hlc"]))
		case node.EventSendSummary:
			out = append(out, fmt.Sprintf("%s%-6s%s %s SUMMARY%s → %s peer=%v leaves=%v root=%v",
				clrGray, e.Node, clrReset, clrCyan, clrReset, clrDim, e.Fields["peer"], e.Fields["leaves"], e.Fields["root"]))
		case node.EventSendReq:
			out = append(out, fmt.Sprintf("%s%-6s%s %s REQ%s needs=%s",
				clrGray, e.Node, clrReset, clrYellow, clrReset, colorize(fmtNeeds(e.Fields["needs"]), clrYellow)))
		case node.EventSendDelta:
			bytesField := e.Fields["bytes"]
			out = append(out, fmt.Sprintf("%s%-6s%s %s DELTA%s entries=%v ops=%v bytes=%v",
				clrGray, e.Node, clrReset, clrMagenta, clrReset, e.Fields["entries"], e.Fields["ops"], bytesField))
		case node.EventAppliedDelta:
			out = append(out, fmt.Sprintf("%s%-6s%s %s APPLIED%s keys=%v before=%s after=%s",
				clrGray, e.Node, clrReset, clrGreen, clrReset,
				e.Fields["keys"], fmtCounts(e.Fields["before"]), fmtCounts(e.Fields["after"])))
		case node.EventConnChange:
			up := e.Fields["up"] == true
			col := clrRed
			label := "down"
			if up {
				col = clrGreen
				label = "up"
			}
			out = append(out, fmt.Sprintf("%s%-6s%s %s LINK %s", clrGray, e.Node, clrReset, col, label))
		case node.EventPauseChange:
			on := e.Fields["paused"] == true
			col := clrGreen
			label := "off"
			if on {
				col = clrYellow
				label = "on"
			}
			out = append(out, fmt.Sprintf("%s%-6s%s %s PAUSE %s", clrGray, e.Node, clrReset, col, label))
		case node.EventWarn:
			out = append(out, fmt.Sprintf("%s%-6s%s %s WARN%s %v", clrGray, e.Node, clrReset, clrRed, clrReset, e.Fields))
		case node.EventSendSegAd:
			out = append(out, fmt.Sprintf("%s%-6s%s %s SEG_AD%s items=%v",
				clrGray, e.Node, clrReset, clrCyan, clrReset, e.Fields["items"]))

		case node.EventSendSegKeysReq:
			out = append(out, fmt.Sprintf("%s%-6s%s %s SEG_KEYS_REQ%s sids=%v",
				clrGray, e.Node, clrReset, clrYellow, clrReset, e.Fields["sids"]))

		case node.EventSendSegKeys:
			out = append(out, fmt.Sprintf("%s%-6s%s %s SEG_KEYS%s items=%v",
				clrGray, e.Node, clrReset, clrCyan, clrReset, e.Fields["items"]))

		case node.EventHB:
			// ignore to reduce noise
			// (do nothing)
			continue
		default:
			out = append(out, fmt.Sprintf("%s%-6s%s %s %v", clrGray, e.Node, clrReset, string(e.Type), e.Fields))
		}
	}
	return out
}

func fmtNeeds(v any) string {
	ns, ok := v.([]syncproto.Need)
	if !ok {
		return fmt.Sprint(v)
	}
	if len(ns) == 0 {
		return "[]"
	}
	keys := make([]string, 0, len(ns))
	for _, n := range ns {
		keys = append(keys, fmt.Sprintf("%s:%d", n.Key, n.From))
	}
	return "[" + strings.Join(keys, " ") + "]"
}

func fmtCounts(v any) string {
	m, ok := v.(map[string]int)
	if !ok || len(m) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", k, m[k]))
	}
	return "{" + strings.Join(parts, " ") + "}"
}

// ----- panel, diff, ui helpers -----

func diffKeys(a, b []merkle.Leaf) []string {
	am := make(map[string][32]byte, len(a))
	bm := make(map[string][32]byte, len(b))
	for _, lf := range a {
		am[lf.Key] = lf.Hash
	}
	for _, lf := range b {
		bm[lf.Key] = lf.Hash
	}
	seen := map[string]struct{}{}
	for k := range am {
		seen[k] = struct{}{}
	}
	for k := range bm {
		seen[k] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		if am[k] != bm[k] {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func shortHex(b []byte, n int) string {
	const hexdigits = "0123456789abcdef"
	out := make([]byte, 0, 2*n)
	for i := 0; i < len(b) && len(out) < 2*n; i++ {
		out = append(out, hexdigits[b[i]>>4], hexdigits[b[i]&0x0f])
	}
	return string(out)
}

func stringsRepeat(s string, n int) string {
	var b bytes.Buffer
	for range n {
		b.WriteString(s)
	}
	return b.String()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func truncateList(keys []string, max int) string {
	if len(keys) == 0 {
		return ""
	}
	if len(keys) <= max {
		return fmt.Sprintf("(%v)", keys)
	}
	return fmt.Sprintf("(%v … +%d)", keys[:max], len(keys)-max)
}

// printFull clears the entire line (y) and prints s from column 1
func printFull(y int, s string) {
	fmt.Printf("\x1b[%d;1H\x1b[2K%s", y+1, s)
}

// printBox prints inside a fixed-width column starting at (x,y) without clearing the other column
func printBox(x, y, w int, s string) {
	fmt.Printf("\x1b[%d;%dH", y+1, x+1) // move
	if visLen(s) > w {
		s = truncateToWidth(s, w)
	}
	fmt.Print(s)
	if p := w - visLen(s); p > 0 {
		fmt.Print(stringsRepeat(" ", p))
	}
}

// visible length (strip ANSI escape sequences crudely)
func visLen(s string) int { return len(stripANSI(s)) }

func truncateToWidth(s string, w int) string {
	if visLen(s) <= w {
		return s
	}
	if w <= 1 {
		return s[:w]
	}
	return s[:w-1] + "…"
}

func stripANSI(s string) string {
	out := make([]byte, 0, len(s))
	inEsc := false
	for i := range len(s) {
		c := s[i]
		if inEsc {
			if c >= '@' && c <= '~' { // end of CSI
				inEsc = false
			}
			continue
		}
		if c == 0x1b {
			inEsc = true
			continue
		}
		out = append(out, c)
	}
	return string(out)
}

func clearScreen() { fmt.Print("\x1b[2J\x1b[H") }

func installUI() {
	// fmt.Print("\x1b[?1049h")   // ALT SCREEN ON
	fmt.Print("\x1b[?25l")     // hide cursor
	fmt.Print("\x1b[H\x1b[2J") // home + clear
}

// leave alt screen + show cursor + reset attrs
func restoreUI() {
	fmt.Print("\x1b[0m")   // reset SGR
	fmt.Print("\x1b[?25h") // show cursor
	// fmt.Print("\x1b[?1049l\n") // ALT SCREEN OFF (restores previous screen)
}

func onOff(b bool) string {
	if b {
		return colorize("up", clrGreen)
	}
	return colorize("down", clrRed)
}

func colorize(s, color string) string { return color + s + clrReset }

func ifThen[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

func atoiSafe(s string) int {
	n := 0
	for _, r := range s {
		if r >= '0' && r <= '9' {
			n = n*10 + int(r-'0')
		} else {
			break
		}
	}
	return n
}

func randKey() string {
	const letters = "ABCDEFGH"
	i := time.Now().UnixNano() % int64(len(letters))
	return string(letters[i])
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func healthLabel(n *node.Node) string {
	if n.IsPaused() {
		return colorize("suspect", clrRed)
	}
	return colorize("healthy", clrGreen)
}

func diffSegments(a, b map[segment.ID][32]byte) []segment.ID {
	seen := map[segment.ID]struct{}{}
	for sid := range a {
		seen[sid] = struct{}{}
	}
	for sid := range b {
		seen[sid] = struct{}{}
	}
	out := make([]segment.ID, 0, len(seen))
	for sid := range seen {
		if a[sid] != b[sid] {
			out = append(out, sid)
		}
	}
	slices.Sort(out)
	return out
}

func truncateSegList(sids []segment.ID, max int) string {
	if len(sids) == 0 {
		return ""
	}
	if len(sids) <= max {
		return fmt.Sprintf("(%v)", sids)
	}
	head := make([]segment.ID, len(sids[:max]))
	copy(head, sids[:max])
	return fmt.Sprintf("(%v … +%d)", head, len(sids)-max)
}
