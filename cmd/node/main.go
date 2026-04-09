package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/arbhalerao/otter/raft"
)

func main() {
	id := flag.Int("id", 0, "node id (0-based)")
	addr := flag.String("addr", "", "listen address (e.g. :5000)")
	peers := flag.String("peers", "", "comma-separated id=host:port pairs (e.g. 1=localhost:5001,2=localhost:5002)")
	data := flag.String("data", "", "data directory for persistence")
	flag.Parse()

	if *addr == "" || *peers == "" {
		fmt.Fprintln(os.Stderr, "usage: node -id=N -addr=:PORT -peers=ID=host:port,... [-data=DIR]")
		os.Exit(1)
	}

	peerMap := make(map[int32]string)
	for _, p := range strings.Split(*peers, ",") {
		parts := strings.SplitN(p, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("invalid peer format: %q (expected id=host:port)", p)
		}
		pid, err := strconv.Atoi(parts[0])
		if err != nil {
			log.Fatalf("invalid peer id: %q", parts[0])
		}
		peerMap[int32(pid)] = parts[1]
	}

	if *data == "" {
		*data = fmt.Sprintf("/tmp/raft-node-%d", *id)
	}

	rf := raft.New(int32(*id), *addr, peerMap, *data)
	if err := rf.Start(); err != nil {
		log.Fatalf("failed to start node %d: %v", *id, err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Printf("[node %d] shutting down", *id)
	rf.Stop()
}
