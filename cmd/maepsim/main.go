package main

import (
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/jroimartin/gocui"
	"github.com/juanpablocruz/maepsim/internal/node"
	"github.com/juanpablocruz/maepsim/internal/tui"
)

func runSimulation(simDone chan struct{}) {
	// Create a ring of nodes.
	const numNodes = 5
	basePort := 8000
	var nodes []*node.Node
	for i := 0; i < numNodes; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", basePort+i)
		nodeID := fmt.Sprintf("node-%d", i)
		n := node.New(nodeID, addr)
		nodes = append(nodes, n)
		// Update tui log and status.
		tui.AppendLog(fmt.Sprintf("Node started %s at %s", nodeID, addr))
	}

	// Establish the ring: set each node’s successor and predecessor.
	for i := 0; i < numNodes; i++ {
		nodes[i].Succ = nodes[(i+1)%numNodes].Address
		nodes[i].Pred = nodes[(i-1+numNodes)%numNodes].Address
	}
	// Mark all nodes as online in our TUI global state.
	var statuses []tui.NodeStatus
	for _, n := range nodes {
		statuses = append(statuses, tui.NodeStatus{
			ID:      n.ID,
			Address: n.Address,
			Online:  true,
			Succ:    n.Succ,
			Pred:    n.Pred,
		})
	}
	tui.UpdateNodesStatus(statuses)

	// Start all nodes.
	for _, n := range nodes {
		n.Start()
	}

	// Allow nodes some time to start.
	time.Sleep(2 * time.Second)

	var wg sync.WaitGroup
	stopSim := make(chan struct{})

	// Simulate writes every 3 seconds.
	wg.Add(1)
	go func() {
		defer wg.Done()
		keys := []string{"key1", "key2", "key3", "key4", "key5"}
		for {
			select {
			case <-stopSim:
				return
			case <-time.After(3 * time.Second):
				idx := rand.Intn(len(nodes))
				key := keys[rand.Intn(len(keys))]
				value := fmt.Sprintf("value-%d", rand.Intn(1000))
				nodes[idx].State.AddData(key, value)
				tui.AppendLog(fmt.Sprintf("Simulated write on %s: %s=%s", nodes[idx].ID, key, value))
			}
		}
	}()

	// Simulate periodic sync operations every 7 seconds.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(7 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stopSim:
				return
			case <-ticker.C:
				for _, n := range nodes {
					go n.SyncWithSuccessor()
				}
			}
		}
	}()

	// Simulate nodes going offline and later rejoining.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopSim:
				return
			case <-time.After(10 * time.Second):
				idx := rand.Intn(len(nodes))
				tui.AppendLog(fmt.Sprintf("Simulated node offline %s", nodes[idx].ID))
				nodes[idx].Stop()
				// Update TUI: mark as offline.
				statuses := tui.GetNodesStatus()
				for i, s := range statuses {
					if s.ID == nodes[idx].ID {
						statuses[i].Online = false
					}
				}
				tui.UpdateNodesStatus(statuses)
				// After a delay, bring the node back online.
				go func(n *node.Node) {
					offlineDuration := time.Duration(5+rand.Intn(5)) * time.Second
					time.Sleep(offlineDuration)
					n.Start()
					tui.AppendLog(fmt.Sprintf("Node rejoined network %s", n.ID))
					// Update TUI: mark as online.
					statuses := tui.GetNodesStatus()
					for i, s := range statuses {
						if s.ID == n.ID {
							statuses[i].Online = true
						}
					}
					tui.UpdateNodesStatus(statuses)
				}(nodes[idx])
			}
		}
	}()

	// Simulate a new node joining after 20 seconds.
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Second)
		newNode := node.New("node-new", "127.0.0.1:9000")
		// Join via a random existing node.
		joinTarget := nodes[rand.Intn(len(nodes))].Address
		newNode.SendJoin(joinTarget)
		newNode.Start()
		nodes = append(nodes, newNode)
		tui.AppendLog(fmt.Sprintf("New node joined %s", newNode.ID))

		statuses := tui.GetNodesStatus()
		statuses = append(statuses, tui.NodeStatus{
			ID:      newNode.ID,
			Address: newNode.Address,
			Online:  true,
			Succ:    newNode.Succ,
			Pred:    newNode.Pred,
		})
		tui.UpdateNodesStatus(statuses)
	}()

	// Run the simulation for 60 seconds.
	simDuration := 60 * time.Second
	tui.AppendLog(fmt.Sprintf("Simulation running for %v...", simDuration))
	time.Sleep(simDuration)
	close(stopSim)
	wg.Wait()

	tui.AppendLog("Simulation finished.")
	close(simDone)
}

func layout(g *gocui.Gui) error {
	maxX, maxY := g.Size()

	// Top view for network graph.
	if v, err := g.SetView("graph", 0, 0, maxX-1, maxY/2-1); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Title = "Network Graph"
		v.Wrap = true
	}

	// Bottom view for logs.
	if v, err := g.SetView("logs", 0, maxY/2, maxX-1, maxY-1); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Title = "Logs"
		v.Wrap = true
	}
	return nil
}

func refreshViews(g *gocui.Gui) error {
	// Update the graph view.
	if gv, err := g.View("graph"); err == nil {
		gv.Clear()
		statuses := tui.GetNodesStatus()
		for _, ns := range statuses {
			status := "offline"
			if ns.Online {
				status = "online"
			}
			fmt.Fprintf(gv, "%s (%s)  |  Pred: %s  |  Succ: %s\n", ns.ID, status, ns.Pred, ns.Succ)
		}
	}

	// Update the logs view.
	if lv, err := g.View("logs"); err == nil {
		lv.Clear()
		logs := tui.GetLogBuffer()
		for _, line := range logs {
			fmt.Fprintln(lv, line)
		}
		// Auto-scroll: if there are more lines than the view's height, adjust the origin.
		_, viewHeight := lv.Size()
		if len(logs) > viewHeight {
			lv.SetOrigin(0, len(logs)-viewHeight)
		} else {
			lv.SetOrigin(0, 0)
		}
	}
	return nil
}

// Main integration: run simulation and TUI concurrently.
func main() {
	// Channel to signal simulation completion.
	simDone := make(chan struct{})

	// Start the simulation in a separate goroutine.
	go runSimulation(simDone)

	g, err := gocui.NewGui(gocui.OutputNormal)
	if err != nil {
		slog.Default().Error("Failed to create GUI", "error", err)
		return
	}
	defer g.Close()
	g.SetManagerFunc(layout)

	// Bind Ctrl+C to force an exit.
	if err := g.SetKeybinding("", gocui.KeyCtrlC, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
		os.Exit(0)
		return nil
	}); err != nil {
		slog.Default().Error("Failed to set Ctrl+C key binding", "error", err)
	}

	go func() {
		for {
			time.Sleep(1 * time.Second)
			g.Update(func(g *gocui.Gui) error {
				return refreshViews(g)
			})
		}
	}()

	if err := g.MainLoop(); err != nil && err != gocui.ErrQuit {
		slog.Default().Error("GUI main loop error", "error", err)
	}

	<-simDone
}
