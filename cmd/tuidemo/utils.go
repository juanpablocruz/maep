package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"

	"github.com/juanpablocruz/maep/pkg/node"
	"github.com/juanpablocruz/maep/pkg/wire"
)

func writeJSONFile(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func formatEvent(e node.Event) string {
	switch e.Type {
	case node.EventPut:
		return fmt.Sprintf("PUT  %s: %s=%s", e.Node, e.Fields["key"], e.Fields["val"])
	case node.EventDel:
		return fmt.Sprintf("DEL  %s: %s", e.Node, e.Fields["key"])
	case node.EventSync:
		return fmt.Sprintf("SYNC %s: %s", e.Node, e.Fields["action"])
	case node.EventConnChange:
		up := e.Fields["up"] == true
		return fmt.Sprintf("LINK %s: %s", e.Node, ifThen(up, "up", "down"))
	case node.EventPauseChange:
		paused := e.Fields["paused"] == true
		return fmt.Sprintf("PAUSE %s: %s", e.Node, ifThen(paused, "on", "off"))
	default:
		if string(e.Type) == "cmd" {
			if s, ok := e.Fields["cmd"].(string); ok {
				return "> " + s
			}
		}
		return fmt.Sprintf("%s %s: %v", e.Type, e.Node, e.Fields)
	}
}

func formatWireEvent(e node.Event) string {
	proto, _ := e.Fields["proto"].(string)
	dir, _ := e.Fields["dir"].(string)
	mt, _ := e.Fields["mt"].(int)
	bytes, _ := e.Fields["bytes"].(int)
	name := mtName(byte(mt))
	return fmt.Sprintf("%s %s %s %dB", proto, dir, name, bytes)
}

func mtName(mt byte) string {
	switch mt {
	case wire.MT_SYNC_SUMMARY_REQ:
		return "SUMMARY_REQ"
	case wire.MT_SYNC_SUMMARY_RESP:
		return "SUMMARY_RESP"
	case wire.MT_SYNC_REQ:
		return "REQ"
	case wire.MT_SYNC_DELTA:
		return "DELTA"
	case wire.MT_PING:
		return "PING"
	case wire.MT_PONG:
		return "PONG"
	case wire.MT_SEG_AD:
		return "SEG_AD"
	case wire.MT_SEG_KEYS_REQ:
		return "SEG_KEYS_REQ"
	case wire.MT_SEG_KEYS:
		return "SEG_KEYS"
	case wire.MT_SYNC_BEGIN:
		return "BEGIN"
	case wire.MT_SYNC_END:
		return "END"
	default:
		return fmt.Sprintf("MT_%d", mt)
	}
}

func generateRandomKey() string {
	i := rand.Intn(len(writerKeys))
	return string(writerKeys[i])
}

func shortHex(b []byte, n int) string {
	const hexdigits = "0123456789abcdef"
	out := make([]byte, 0, 2*n)
	for i := 0; i < len(b) && len(out) < 2*n; i++ {
		out = append(out, hexdigits[b[i]>>4], hexdigits[b[i]&0x0f])
	}
	return string(out)
}

func ifThen[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
