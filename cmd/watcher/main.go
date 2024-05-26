package main

import (
	"bufio"
	"context"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/juanpablocruz/maep/internal/network"
	pNetwork "github.com/libp2p/go-libp2p/core/network"

	golog "github.com/ipfs/go-log/v2"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// LibP2P code uses golog to log messages. They log with different
	// string IDs (i.e. "swarm"). We can control the verbosity level for
	// all loggers with:
	golog.SetAllLoggers(golog.LevelInfo) // Change to INFO for extra info

	// Parse options from the command line
	listenF := flag.Int("l", 0, "wait for incoming connections")
	flag.Parse()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	if *listenF == 0 {
		log.Fatal("Please provide a port to bind on with -l")
	}

	// Make a host that listens on the given multiaddress

	n := network.NewNetworkNode(listenF, false)

	n.StartListener(ctx, doEcho)

	select {
	case <-ctx.Done():
		log.Println("Shutting down...")
	case <-sigChan:
		log.Println("Shutting down...")
	}
}

// doEcho reads a line of data a stream and writes it back
func doEcho(s pNetwork.Stream) error {
	buf := bufio.NewReader(s)
	payload := make([]byte, 1024)
	_, err := io.ReadFull(buf, payload)
	if err != nil {
		return err
	}

	log.Printf("read: %s", payload)
	_, err = s.Write(payload)
	return err
}
