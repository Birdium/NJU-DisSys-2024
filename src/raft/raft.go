package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"dissys/src/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

)

// import "bytes"
// import "encoding/gob"

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

const (
	HEARTBEAT_INTERVAL   = 100
	ELECTION_TIMEOUT_MIN = 150
	ELECTION_TIMEOUT_MAX = 300
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	A int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	role int

	// Timer 
	electionTimer 	*time.Timer
	heartbeatTimer 	*time.Timer

	// channel of vote results
	voteCh	    chan(bool)
}

func getRandElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN) * time.Millisecond
}

func getHeartbeatTimeout() time.Duration {
	return HEARTBEAT_INTERVAL * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	// entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		DPrintf("Node %v Term %v refused to vote for Node %v Term %v", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}
	
	if args.Term > rf.currentTerm {
		if rf.role != FOLLOWER {
			DPrintf("Node %v has become a follower with term %v!", rf.me, args.Term)
		}
		rf.role = FOLLOWER
		rf.votedFor = -1
	}
	
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
	
	rf.resetElectionTimer()
	if (rf.votedFor == args.CandidateId) {
		DPrintf("Node %v Term %v voted for Node %v Term %v", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	} else {
		DPrintf("Node %v Term %v refused to vote for Node %v Term %v", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	}
}

// Append Entries RPC handler.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO: phase 2B
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Node %v Term %v receives a heartbeat from Node %v Term %v", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	
	if rf.role != FOLLOWER {
		DPrintf("Node %v has become a follower with term %v!", rf.me, args.Term)
	}

	rf.role = FOLLOWER
	

	rf.resetElectionTimer()
}

func (rf *Raft) getRequestVoteArgs() RequestVoteArgs {
	args := RequestVoteArgs {
		Term : rf.currentTerm,
		CandidateId : rf.me,
		// TODO: phase 2B
		LastLogIndex: -1,
		LastLogTerm : -1,
	}
	return args
}

func (rf *Raft) becomeLeader() {
    DPrintf("Node %v becomes a leader", rf.me)

	rf.role = LEADER
	rf.electionTimer.Reset(getRandElectionTimeout())
	rf.heartbeatTimer.Reset(0)

	npeer := len(rf.peers)
	nlog := len(rf.log) 

	rf.nextIndex = make([]int, npeer)
	rf.matchIndex = make([]int, npeer)

	for i := range rf.peers {
		rf.nextIndex[i] = nlog
		rf.matchIndex[i] = 0
	}

	rf.nextIndex[rf.me] = nlog - 1

	// rf.persist()

}

// This func is called when rf.mu is held by the caller
// will release rf.mu
func (rf *Raft) startElection() {
	rf.role = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.resetElectionTimer()

	DPrintf("Node %v has become a candidate with term %v!", rf.me, rf.currentTerm)

	args := rf.getRequestVoteArgs()

	npeer := len(rf.peers)

	voteCh := make(chan bool, npeer)

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// start a goroutine that sends RequestVote RPC to peer
		// vote result will be transferred via channel voteCh 
		go func(peer int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, args, &reply)
			if ok {
				voteCh <- reply.VoteGranted
			} else {
				voteCh <- false
			}
		} (peer)
	}

	go func() {
		voteTot := 1
		voteCnt := 1
	
		// loop that counts the result
		for {
			vote := <-voteCh
			voteTot += 1
			if vote {
				voteCnt += 1
			}
			if voteTot == npeer || voteCnt > npeer/2 || voteTot-voteCnt > npeer/2 {
				break
			}
		}
	
		if voteCnt > npeer/2 {
			rf.becomeLeader()
		}
	}()
}

func (rf *Raft) getAppendEntriesArgs() AppendEntriesArgs {
	args := AppendEntriesArgs{
		Term : rf.currentTerm,
		LeaderId : rf.me,
	}
	return args
}

// leader send heartbeats to all 
func (rf *Raft) sendHeartbeats() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		args := rf.getAppendEntriesArgs()
		go func(peer int) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(peer, args, &reply)
			if ok {
				// TODO: 
			} else {
				// TODO:
			}
		} (peer)
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Reset(getRandElectionTimeout())
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <- rf.electionTimer.C:
			rf.resetElectionTimer()
			// election timeout, start election
			rf.mu.Lock()
			if rf.role == LEADER {
				// debug print
				rf.mu.Unlock()
				continue
			}
			DPrintf("Node %v election timeout", rf.me)
			rf.startElection()
			rf.mu.Unlock()
		case <- rf.heartbeatTimer.C:
			// heartbeat timeout, send heart beat
			rf.mu.Lock()
			if rf.role == LEADER {
				rf.sendHeartbeats()
				rf.heartbeatTimer.Reset(getHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	
	// Your initialization code here.
	
	// initialize basic information
	rf.role = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.dead = 0

	// initialize timer
	rf.electionTimer = time.NewTimer(getRandElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(HEARTBEAT_INTERVAL * time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
