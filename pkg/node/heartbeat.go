package node

import (
	"math/rand"
	"time"

	"github.com/juanpablocruz/maep/pkg/protoport"
	"github.com/juanpablocruz/maep/pkg/wire"
)

func (n *Node) heartbeatLoop() {
	t := time.NewTicker(n.hbEvery)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
			if !n.conn.Load() {
				n.onMiss()
				continue
			}

			// Small jitter to avoid lockstep heartbeats when many nodes co-reside
			if n.hbEvery > 0 {
				maxJ := n.hbEvery / 10
				if maxJ > 25*time.Millisecond {
					maxJ = 25 * time.Millisecond
				}
				if maxJ > 0 {
					d := time.Duration(rand.Int63n(int64(maxJ)))
					time.Sleep(d)
				}
			}

			// Prefer messenger for HB if available
			if n.hbms != nil {
				if err := n.hbms.Send(n.ctx, n.Peer, protoport.PingMsg{}); err != nil {
					dir := "->udp"
					if n.HB == nil {
						dir = "->tcp"
					}
					n.emit(EventHB, map[string]any{"dir": dir, "err": err.Error()})
					n.onMiss()
					continue
				}
			} else if err := n.sendHB(wire.MT_PING, nil); err != nil {
				dir := "->udp"
				if n.HB == nil {
					dir = "->tcp"
				}
				n.emit(EventHB, map[string]any{"dir": dir, "err": err.Error()})
				n.onMiss()
				continue
			}
			{
				dir := "->udp"
				if n.HB == nil {
					dir = "->tcp"
				}
				n.emit(EventHB, map[string]any{"dir": dir})
			}
		}
	}
}
