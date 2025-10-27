package raft

import (
	"context"
	"log"
	"sort"
	"time"

	pb "github.com/arbhalerao/raft/proto"
)

func (rf *Rf) heartbeatLoop() {
	for {
		select {
		case <-rf.stopCh:
			return
		case <-rf.hbTicker.C:
			rf.mu.Lock()
			isLeader := rf.role == Leader
			rf.mu.Unlock()
			if !isLeader {
				return
			}
			rf.replicateAll()
		}
	}
}

func (rf *Rf) replicateAll() {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for _, p := range rf.peers {
		go rf.replicate(p.ID, p.Addr)
	}
}

func (rf *Rf) replicate(pid int32, addr string) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}

	next := rf.ni[pid]
	prevIdx := next - 1
	prevTerm := int32(0)
	if prevIdx >= 0 && int(prevIdx) < len(rf.lg) {
		prevTerm = rf.lg[prevIdx].Term
	}

	var entries []*pb.LogEntry
	if int(next) < len(rf.lg) {
		for _, e := range rf.lg[next:] {
			entries = append(entries, &pb.LogEntry{
				Term:    e.Term,
				Index:   e.Idx,
				Command: e.Cmd,
			})
		}
	}

	req := &pb.AppendRequest{
		Term:         rf.ct,
		LeaderId:     rf.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.ci,
	}
	term := rf.ct
	rf.mu.Unlock()

	c, err := rf.getClient(addr)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	resp, err := c.AppendEntries(ctx, req)
	if err != nil {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.ct != term || rf.role != Leader {
		return
	}

	if resp.Term > rf.ct {
		log.Printf("[node %d] discovered higher term %d, stepping down", rf.id, resp.Term)
		rf.becomeFollower(resp.Term)
		return
	}

	if resp.Success {
		if len(entries) > 0 {
			lastNew := entries[len(entries)-1].Index
			rf.ni[pid] = lastNew + 1
			rf.mi[pid] = lastNew
			log.Printf("[node %d] replicated log up to index %d to node %d", rf.id, lastNew, pid)
			rf.advanceCommit()
		}
	} else {
		if rf.ni[pid] > 1 {
			rf.ni[pid]--
		}
		go rf.replicate(pid, addr)
	}
}

func (rf *Rf) advanceCommit() {
	matches := make([]int, 0, len(rf.peers)+1)
	matches = append(matches, int(rf.mi[rf.id]))
	for _, p := range rf.peers {
		matches = append(matches, int(rf.mi[p.ID]))
	}
	sort.Sort(sort.Reverse(sort.IntSlice(matches)))

	majority := len(matches)/2 + 1
	n := int32(matches[majority-1])

	if n > rf.ci && rf.lg[n].Term == rf.ct {
		log.Printf("[node %d] advanced commitIndex from %d to %d", rf.id, rf.ci, n)
		rf.ci = n
	}
}
