package raft

import (
	"log"
	"strings"
	"time"
)

func (rf *Rf) applyLoop() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rf.stopCh:
			return
		case <-ticker.C:
			rf.applyCommitted()
		}
	}
}

func (rf *Rf) applyCommitted() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.la < rf.ci {
		rf.la++
		e := rf.lg[rf.la]
		rf.applyEntry(e)
	}
}

func (rf *Rf) applyEntry(e Entry) {
	if e.Cmd == "" {
		return
	}
	parts := strings.SplitN(e.Cmd, " ", 3)
	switch strings.ToUpper(parts[0]) {
	case "SET":
		if len(parts) >= 3 {
			rf.sm[parts[1]] = parts[2]
			log.Printf("[node %d] applied index %d: SET %s = %s", rf.id, e.Idx, parts[1], parts[2])
		}
	case "DEL":
		if len(parts) >= 2 {
			delete(rf.sm, parts[1])
			log.Printf("[node %d] applied index %d: DEL %s", rf.id, e.Idx, parts[1])
		}
	default:
		log.Printf("[node %d] applied index %d: %s (unknown cmd, stored as-is)", rf.id, e.Idx, e.Cmd)
	}
}
