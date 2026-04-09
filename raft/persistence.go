package raft

import (
	"log"

	"github.com/arbhalerao/otter/storage"
)

func (rf *Rf) persist() {
	entries := make([]storage.LogEntry, len(rf.lg))
	for i, e := range rf.lg {
		entries[i] = storage.LogEntry{
			Term:  e.Term,
			Index: e.Idx,
			Cmd:   e.Cmd,
		}
	}
	p := &storage.Persistent{
		CurrentTerm: rf.ct,
		VotedFor:    rf.vf,
		Log:         entries,
	}
	if err := rf.store.Save(p); err != nil {
		log.Printf("[node %d] persist error: %v", rf.id, err)
	}
}

func (rf *Rf) restore() error {
	p, err := rf.store.Load()
	if err != nil {
		return err
	}
	rf.ct = p.CurrentTerm
	rf.vf = p.VotedFor
	if len(p.Log) > 0 {
		rf.lg = make([]Entry, len(p.Log))
		for i, e := range p.Log {
			rf.lg[i] = Entry{Term: e.Term, Idx: e.Index, Cmd: e.Cmd}
		}
	}
	return nil
}
