package raft

import (
	"context"
	"log"
	"time"

	pb "github.com/arbhalerao/otter/proto"
)

func (rf *Rf) electLoop() {
	for {
		select {
		case <-rf.stopCh:
			return
		case <-rf.electTimer.C:
			rf.startElection()
		}
	}
}

func (rf *Rf) startElection() {
	rf.mu.Lock()

	if rf.role == Leader {
		rf.resetElectTimer()
		rf.mu.Unlock()
		return
	}

	rf.role = Candidate
	rf.ct++
	rf.vf = rf.id
	rf.leader = -1
	rf.persist()

	term := rf.ct
	lastIdx, lastTerm := rf.lastLogInfo()
	rf.resetElectTimer()

	log.Printf("[node %d] starting election for term %d", rf.id, term)
	rf.mu.Unlock()

	votes := 1
	total := len(rf.peers) + 1
	majority := total/2 + 1

	for _, p := range rf.peers {
		go func(peer Peer) {
			c, err := rf.getClient(peer.Addr)
			if err != nil {
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			resp, err := c.RequestVote(ctx, &pb.VoteRequest{
				Term:         term,
				CandidateId:  rf.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.ct != term || rf.role != Candidate {
				return
			}

			if resp.Term > rf.ct {
				log.Printf("[node %d] discovered higher term %d, stepping down", rf.id, resp.Term)
				rf.becomeFollower(resp.Term)
				return
			}

			if resp.VoteGranted {
				log.Printf("[node %d] received vote from node %d for term %d", rf.id, peer.ID, term)
				votes++
				if votes >= majority {
					rf.becomeLeader()
				}
			}
		}(p)
	}
}
