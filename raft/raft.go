package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	pb "github.com/arbhalerao/otter/proto"
	"github.com/arbhalerao/otter/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	minElectTimeout = 150
	maxElectTimeout = 300
	heartbeatMs     = 50
)

type Peer struct {
	ID   int32
	Addr string
}

type Rf struct {
	mu sync.Mutex

	id    int32
	peers []Peer
	addr  string

	// persistent state
	ct int32
	vf int32
	lg []Entry

	// volatile state
	ci int32
	la int32

	// leader-only state
	ni map[int32]int32
	mi map[int32]int32

	role   Role
	leader int32

	sm map[string]string

	electTimer *time.Timer
	hbTicker   *time.Ticker
	stopCh     chan struct{}

	store *storage.Store
	srv   *grpc.Server

	clients   map[string]pb.RaftClient
	clientsMu sync.Mutex
}

func New(id int32, addr string, peerAddrs map[int32]string, dataDir string) *Rf {
	peers := make([]Peer, 0, len(peerAddrs))
	for pid, paddr := range peerAddrs {
		peers = append(peers, Peer{ID: pid, Addr: paddr})
	}

	rf := &Rf{
		id:      id,
		addr:    addr,
		peers:   peers,
		ct:      0,
		vf:      -1,
		ci:      0,
		la:      0,
		role:    Follower,
		leader:  -1,
		sm:      make(map[string]string),
		stopCh:  make(chan struct{}),
		store:   storage.New(dataDir),
		clients: make(map[string]pb.RaftClient),
	}

	rf.lg = []Entry{{Term: 0, Idx: 0, Cmd: ""}}
	return rf
}

func (rf *Rf) Start() error {
	if err := rf.restore(); err != nil {
		return fmt.Errorf("restore: %w", err)
	}

	lis, err := net.Listen("tcp", rf.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", rf.addr, err)
	}

	rf.srv = grpc.NewServer()
	pb.RegisterRaftServer(rf.srv, &rpcServer{rf: rf})

	go func() {
		if err := rf.srv.Serve(lis); err != nil {
			log.Printf("[node %d] grpc serve: %v", rf.id, err)
		}
	}()

	rf.resetElectTimer()
	go rf.electLoop()
	go rf.applyLoop()

	log.Printf("[node %d] started as %s at %s", rf.id, rf.role, rf.addr)
	return nil
}

func (rf *Rf) Stop() {
	close(rf.stopCh)
	if rf.hbTicker != nil {
		rf.hbTicker.Stop()
	}
	if rf.electTimer != nil {
		rf.electTimer.Stop()
	}
	if rf.srv != nil {
		rf.srv.GracefulStop()
	}
}

func (rf *Rf) Submit(cmd string) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return false
	}

	e := Entry{
		Term: rf.ct,
		Idx:  int32(len(rf.lg)),
		Cmd:  cmd,
	}
	rf.lg = append(rf.lg, e)
	rf.persist()

	log.Printf("[node %d] leader appended log index %d: %s", rf.id, e.Idx, cmd)

	rf.mi[rf.id] = e.Idx

	go rf.replicateAll()
	return true
}

func (rf *Rf) GetState() (Role, int32, int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role, rf.ct, rf.leader
}

func (rf *Rf) GetSM() map[string]string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	cp := make(map[string]string, len(rf.sm))
	for k, v := range rf.sm {
		cp[k] = v
	}
	return cp
}

func (rf *Rf) ID() int32 {
	return rf.id
}

func (rf *Rf) resetElectTimer() {
	d := time.Duration(minElectTimeout+rand.Intn(maxElectTimeout-minElectTimeout)) * time.Millisecond
	if rf.electTimer == nil {
		rf.electTimer = time.NewTimer(d)
	} else {
		rf.electTimer.Reset(d)
	}
}

func (rf *Rf) lastLogInfo() (int32, int32) {
	last := rf.lg[len(rf.lg)-1]
	return last.Idx, last.Term
}

func (rf *Rf) getClient(addr string) (pb.RaftClient, error) {
	rf.clientsMu.Lock()
	defer rf.clientsMu.Unlock()

	if c, ok := rf.clients[addr]; ok {
		return c, nil
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	c := pb.NewRaftClient(conn)
	rf.clients[addr] = c
	return c, nil
}

func (rf *Rf) becomeFollower(term int32) {
	rf.role = Follower
	rf.ct = term
	rf.vf = -1
	rf.leader = -1
	if rf.hbTicker != nil {
		rf.hbTicker.Stop()
		rf.hbTicker = nil
	}
	rf.persist()
	rf.resetElectTimer()
}

func (rf *Rf) stepDown() {
	rf.role = Follower
	if rf.hbTicker != nil {
		rf.hbTicker.Stop()
		rf.hbTicker = nil
	}
	rf.resetElectTimer()
}

func (rf *Rf) becomeLeader() {
	rf.role = Leader
	rf.leader = rf.id
	if rf.hbTicker != nil {
		rf.hbTicker.Stop()
	}

	lastIdx, _ := rf.lastLogInfo()
	rf.ni = make(map[int32]int32)
	rf.mi = make(map[int32]int32)
	for _, p := range rf.peers {
		rf.ni[p.ID] = lastIdx + 1
		rf.mi[p.ID] = 0
	}
	rf.mi[rf.id] = lastIdx

	log.Printf("[node %d] became leader for term %d", rf.id, rf.ct)

	rf.hbTicker = time.NewTicker(heartbeatMs * time.Millisecond)
	go rf.heartbeatLoop()

	go rf.replicateAll()
}
