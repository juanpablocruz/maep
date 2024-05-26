package main

import (
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/juanpablocruz/maep/internal/feed"
	"github.com/juanpablocruz/maep/internal/maep"
)

func watchFile(path string) error {
	initialStat, err := os.Stat(path)
	if err != nil {
		return err
	}
	for {
		stat, err := os.Stat(path)
		if err != nil {
			return err
		}
		if stat.Size() != initialStat.Size() || stat.ModTime() != initialStat.ModTime() {
			break
		}
		time.Sleep(1 * time.Second)
	}
	return nil
}

func watchAndParse(n *maep.Node, doneChan chan bool) {
	processed_ops := make(map[string]bool)
	defer func() { doneChan <- true }()

	err := watchFile("feed.csv")
	if err != nil {
		panic(err)
	}

	csv, err := feed.Feed()
	if err != nil {
		panic(err)
	}
	for _, record := range csv {
		args := maep.Argument{
			Args: make(map[string]interface{}),
		}
		for i, val := range record[2:] {
			args.Args[string(rune(i))] = val
		}
		var b bytes.Buffer
		gob.NewEncoder(&b).Encode(args)

		op := maep.NewOperation(b.Bytes(), []string{record[1]})

		if _, ok := processed_ops[string(record[0])]; !ok {
			n.AddOperation(op)
			processed_ops[string(record[0])] = true
		}
	}
	fmt.Print("\033[H\033[2J")
	fmt.Println(n.Print())

	var b bytes.Buffer
	gob.NewEncoder(&b).Encode(n.SyncVector)

	err = n.Sync(b.Bytes())
	if err != nil {
		panic(err)
	}
}

func main() {
	// gui.MainGui()
	listenF := flag.Int("l", 0, "wait for incoming connections")
	targetF := flag.String("d", "", "target peer to dial")
	flag.Parse()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	config := maep.NodeConfig{
		Listen:     listenF,
		Target:     targetF,
		Standalone: false,
	}
	n := maep.NewNode()
	doneChan := make(chan bool)

	err := n.JoinNetwork(config)
	if err != nil {
		panic(err)
	}
	for {
		go watchAndParse(n, doneChan)
		select {
		case <-doneChan:
			continue
		case <-sigChan:
			fmt.Println("\nShutting down...")
			return
		}
	}
}
