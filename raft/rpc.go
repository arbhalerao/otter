package raft

import (
	"context"
	"log"

	pb "github.com/arbhalerao/otter/proto"
)

type rpcServer struct {
	pb.UnimplementedRaftServer
	rf *Rf
}

func (s *rpcServer) RequestVote(_ context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	rf := s.rf
	rf.mu.Lock()
	defer rf.mu.Unlock()

	resp := &pb.VoteResponse{Term: rf.ct, VoteGranted: false}

	if req.Term < rf.ct {
		return resp, nil
	}

	if req.Term > rf.ct {
		rf.becomeFollower(req.Term)
		resp.Term = rf.ct
	}

	// grant vote only if the candidate's log is at least as up-to-date as ours
	lastIdx, lastTerm := rf.lastLogInfo()
	logOk := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx)

	if (rf.vf == -1 || rf.vf == req.CandidateId) && logOk {
		rf.vf = req.CandidateId
		rf.persist()
		resp.VoteGranted = true
		rf.resetElectTimer()
		log.Printf("[node %d] voted for node %d in term %d", rf.id, req.CandidateId, rf.ct)
	}

	return resp, nil
}

func (s *rpcServer) AppendEntries(_ context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	rf := s.rf
	rf.mu.Lock()
	defer rf.mu.Unlock()

	resp := &pb.AppendResponse{Term: rf.ct, Success: false}

	if req.Term < rf.ct {
		return resp, nil
	}

	rf.resetElectTimer()

	if req.Term > rf.ct {
		rf.becomeFollower(req.Term)
		resp.Term = rf.ct
	} else if rf.role == Candidate {
		// same-term AppendEntries from legitimate leader, step down
		// without resetting votedFor (we already voted this term)
		rf.stepDown()
	}

	rf.leader = req.LeaderId

	// reject if our log lacks a matching entry at prevLogIndex/prevLogTerm
	if req.PrevLogIndex > 0 {
		if int(req.PrevLogIndex) >= len(rf.lg) {
			return resp, nil
		}
		if rf.lg[req.PrevLogIndex].Term != req.PrevLogTerm {
			return resp, nil
		}
	}

	idx := req.PrevLogIndex + 1
	for i, entry := range req.Entries {
		pos := idx + int32(i)
		if int(pos) < len(rf.lg) {
			if rf.lg[pos].Term != entry.Term {
				rf.lg = rf.lg[:pos]
				rf.lg = append(rf.lg, Entry{Term: entry.Term, Idx: entry.Index, Cmd: entry.Command})
			}
		} else {
			rf.lg = append(rf.lg, Entry{Term: entry.Term, Idx: entry.Index, Cmd: entry.Command})
		}
	}
	rf.persist()

	// advance commitIndex to min(leaderCommit, last local entry)
	if req.LeaderCommit > rf.ci {
		lastNew := int32(len(rf.lg) - 1)
		if req.LeaderCommit < lastNew {
			rf.ci = req.LeaderCommit
		} else {
			rf.ci = lastNew
		}
	}

	resp.Success = true
	return resp, nil
}
