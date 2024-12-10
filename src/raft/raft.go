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
	"sort"
	"bytes"
	"encoding/gob"
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
	Command interface{}
	Term	int
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

	// Timers
	electionTimer 	*time.Timer
	heartbeatTimer 	*time.Timer

	// Channels
	applyCh 	chan ApplyMsg
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
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
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) isLogUptoDate(argLastLogIndex int, argLastLogTerm int) bool {
	return rf.getLastLogTerm() < argLastLogTerm || (rf.getLastLogTerm() == argLastLogTerm && rf.getLastLogIndex() <= argLastLogIndex)
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

	
	doPersist := false

	if args.Term > rf.currentTerm {
		if rf.role != FOLLOWER {
			DPrintf("Node %v has become a follower with term %v!", rf.me, args.Term)
		}
		rf.role = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		doPersist = true
	}
	
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if (rf.isLogUptoDate(args.LastLogIndex, args.LastLogTerm)) {
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			doPersist = true
		}
	}
	
	if doPersist {
		rf.persist()
	}

	if (rf.votedFor == args.CandidateId) {
		rf.resetElectionTimer()
		DPrintf("Node %v Term %v voted for Node %v Term %v", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	} else {
		DPrintf("Node %v Term %v refused to vote for Node %v Term %v", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	}


}

func (rf *Raft) debugLog() {
	DPrintf("Node %v's LOG:", rf.me)
	for i := range rf.log {
		if i == 0 {
			continue
		}
		DPrintf("Index %v: [%v]", i, rf.log[i])
	}
	DPrintf("Node %v now has log of length %v and Term %v", rf.me, len(rf.log), rf.currentTerm)

}

// Append Entries RPC handler.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	DPrintf("Node %v Term %v receives a heartbeat from Node %v Term %v", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	
	doPersist := false
	defer func() {
		if doPersist {
			rf.persist()
		}
	}()

	// 1. reply false if term < currentTerm 
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		doPersist = true
	}

	if rf.role != FOLLOWER {
		DPrintf("Node %v has become a follower with term %v!", rf.me, args.Term)
	}

	rf.role = FOLLOWER

	rf.resetElectionTimer()

	// 2. reply false if log doesn't contain an entry
	// at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("Node %v refuse the Append Entry: Log Mismatch", rf.me)
		DPrintf("ARG PREVLOGINDEX %v", args.PrevLogIndex)
		rf.debugLog()
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	// 3. If an existing entry conflicts with a new one
	// (same index but different term), 
	// delete the existing entry and all that follow it.
	
	// 4. Append any new entries not already in the log
	rf.log = append(rf.log[:args.PrevLogIndex + 1], args.Entries...)
	doPersist = true

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(len(rf.log) - 1, args.LeaderCommit)
	}
	
	reply.Success = true
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

func (rf *Raft) getLastLogIndex() int {
	last := len(rf.log) - 1
	return last
}

func (rf *Raft) getLastLogTerm() int {
	last := len(rf.log) - 1
	return rf.log[last].Term
}

func (rf *Raft) getLastLogIndexTerm() (int, int) {
	return rf.getLastLogIndex(), rf.getLastLogTerm()
}

func (rf *Raft) getRequestVoteArgs() RequestVoteArgs {
	args := RequestVoteArgs {
		Term : rf.currentTerm,
		CandidateId : rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm : rf.getLastLogTerm(),
	}
	return args
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

    DPrintf("Node %v becomes a leader", rf.me)

	rf.role = LEADER
	rf.resetElectionTimer()
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

	rf.persist()

}

func (rf *Raft) collectVote(voteCh chan RequestVoteReply, npeer int) {
	voteTot := 1
	voteCnt := 1

	// loop that counts the result
	for {
		vote := <-voteCh
		voteTot += 1
		if vote.VoteGranted {
			voteCnt += 1
		} else {
			rf.mu.Lock()
			if rf.role != CANDIDATE {
				rf.mu.Unlock()
				break
			}
			if vote.Term > rf.currentTerm {
				rf.currentTerm = vote.Term
				rf.role = FOLLOWER
				rf.votedFor = -1
				rf.persist()
				rf.resetElectionTimer()
				voteCnt = 0
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
		}
		if voteTot == npeer || voteCnt > npeer/2 || voteTot-voteCnt > npeer/2 {
			break
		}
	}

	if voteCnt > npeer/2 {
		rf.becomeLeader()
	}
}

// This func is called when rf.mu is held by the caller
func (rf *Raft) startElection() {
	rf.role = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.resetElectionTimer()

	rf.persist()

	DPrintf("Node %v has become a candidate with term %v!", rf.me, rf.currentTerm)

	args := rf.getRequestVoteArgs()

	npeer := len(rf.peers)

	voteCh := make(chan RequestVoteReply, npeer)

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// start a goroutine that sends RequestVote RPC to peer
		// vote result will be transferred via channel voteCh 
		go func(peer int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(peer, args, &reply)
			voteCh <- reply
		} (peer)
	}

	go rf.collectVote(voteCh, npeer)
}

func (rf *Raft) getAppendLogs(peer int) (prevLogIndex int, prevLogTerm int, entries []LogEntry) {

	nextIndex := rf.nextIndex[peer]
	lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()

	if nextIndex <= 0 || nextIndex > lastLogIndex { // heartbeat only
		prevLogIndex = lastLogIndex
		prevLogTerm = lastLogTerm
		return
	}

	entries = append([]LogEntry{}, rf.log[nextIndex:]...)
	prevLogIndex = nextIndex - 1
	if prevLogIndex == 0 {
		prevLogTerm = 0
	} else {
		prevLogTerm = rf.log[prevLogIndex].Term
	}
	return
}

func (rf *Raft) getAppendEntriesArgs(peer int) AppendEntriesArgs {
	prevLogIndex, preLogTerm, entries := rf.getAppendLogs(peer)
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  preLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func getMajorityMatchIndex(matchIndex []int) int {
	n := len(matchIndex)
	tmp := make([]int, n)
	copy(tmp, matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(tmp)))
	return tmp[n/2]
}

func (rf *Raft) sendAppendEntriesToPeer(peer int) {

	// peerNextIndex := rf.nextIndex[peer]

	rf.mu.Lock()
	args := rf.getAppendEntriesArgs(peer)
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, args, &reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// received higher term reply, switch into follower
		if reply.Term > rf.currentTerm { 
			rf.role = FOLLOWER
			rf.currentTerm = reply.Term
			rf.persist()
			rf.resetElectionTimer()
			return 
		}
		
		if reply.Success {
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			majorityIndex := getMajorityMatchIndex(rf.matchIndex)
			if rf.log[majorityIndex].Term == rf.currentTerm && majorityIndex > rf.commitIndex {
				rf.commitIndex = majorityIndex
			}
		} else {
			rf.nextIndex[peer] -= 1
		}
	} 
}

// leader send heartbeats to all peers
func (rf *Raft) sendHeartbeats() {
	nlog := len(rf.log)

	rf.nextIndex[rf.me] = nlog + 1
	rf.matchIndex[rf.me] = nlog

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.sendAppendEntriesToPeer(peer)
	}

}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(getRandElectionTimeout())
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.heartbeatTimer.Stop()
	rf.heartbeatTimer.Reset(getHeartbeatTimeout())
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
				rf.resetHeartbeatTimer()
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) apply(index int) {
	msg := ApplyMsg{
		Index:       index,
		Command:     rf.log[index].Command,
		UseSnapshot: false,
		Snapshot:    nil,
	}
	DPrintf("Node %v's apply msg [%v, %v]", rf.me, index, rf.log[index].Command)
	rf.applyCh <- msg
}

func (rf *Raft) applyLoop() {
	for {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			rf.apply(rf.lastApplied)
		}
		rf.mu.Unlock()
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
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
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.role == LEADER

	if !isLeader || rf.killed() {
		return -1, -1, false
	}

	index := len(rf.log)
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	rf.persist()
	DPrintf("Client sended a new command %v to Leader %v", command, rf.me)
	rf.debugLog()

	return index, term, true
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
	rf.applyCh = applyCh
	rf.log = []LogEntry{{Term: 0}} // null log

	// initialize timer
	rf.electionTimer = time.NewTimer(getRandElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(getHeartbeatTimeout())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLoop()

	return rf
}
