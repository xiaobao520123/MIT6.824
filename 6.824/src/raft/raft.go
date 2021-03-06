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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type RaftState uint

const (
	Unknown = iota
	Follower
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg
	state     RaftState
	pong      bool

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	// For leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	rf.mu.Unlock()

	isleader = rf.isLeader()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	votedFor := rf.votedFor
	log := make([]LogEntry, len(rf.log))
	copy(log, rf.log)
	rf.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(currentTerm)
	e.Encode(votedFor)
	e.Encode(log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	DPrintf("[%v] persist state, currentTerm=%v, votedFor=%v, log=%+v\n", rf.me, currentTerm, votedFor, log)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).

	// atomic
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf("[%v] error when decoding persister\n", rf.me)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = make([]LogEntry, len(log))
	copy(rf.log, log)
	DPrintf("[%v] read persist, currentTerm=%v, votedFor=%v, rf.log=%+v\n", rf.me, currentTerm, votedFor, rf.log)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	defer rf.persist()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%v] RequestVote from %v, term=%v, currentTerm=%v\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)

	// Rule for all servers 2
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
	}

	// Rule 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// Rule 2.1
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("[%v] already vote for %v, vote denied\n", rf.me, rf.votedFor)
		return
	}

	// Rule 2.2
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.log[lastLogIndex].Term
	if args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {

		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("[%v] candidate %v's log is not at least as up-to-date, vote denied\n", rf.me, args.CandidateId)
		return
	}

	rf.votedFor = args.CandidateId
	rf.pong = true

	reply.Term = rf.currentTerm
	reply.VoteGranted = true
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	defer rf.persist()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer rf.persist()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%v] AppendEntries from %v, args=%+v, currentTerm=%v, state=%v, commitIndex=%v\n", rf.me, args.LeaderId, args, rf.currentTerm, rf.state, rf.commitIndex)

	// Rule for all servers 2
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
	}

	// Rule 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Rule 2
	lastLogIndex := rf.getLastLogIndex()
	localNextIndex := lastLogIndex + 1
	if args.PrevLogIndex > lastLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextIndex = localNextIndex
		DPrintf("[%v] log inconsistent, leader's log is newer, nextIndex should be %v\n", rf.me, reply.NextIndex)
		return
	} else if prevLog := rf.log[args.PrevLogIndex]; prevLog.Term != args.PrevLogTerm {
		for ; localNextIndex >= 1; localNextIndex-- {
			if rf.log[localNextIndex-1].Term == args.PrevLogTerm {
				break
			}
		}
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextIndex = localNextIndex
		DPrintf("[%v] log inconsistent, final agreement at %v\n", rf.me, reply.NextIndex)
		return
	}

	nextIndex := args.PrevLogIndex + 1
	for offset, entry := range args.Entries {
		// Rule 3
		index := nextIndex + offset
		if index <= len(rf.log)-1 {
			if index <= len(rf.log)-1 {
				currentLog := rf.log[index]
				if currentLog.Term != entry.Term {
					DPrintf("[%v] existing log=%+v has same index as new log=%+v but with different term, cut!\n", rf.me, currentLog, entry)
					rf.log = rf.log[:index]
				}
			}
		}

		// Rule 4
		exist := false
		for i := rf.getLastLogIndex(); i >= 0; i-- {
			log := rf.log[i]
			if entry.Command == log.Command && entry.Term == log.Term {
				exist = true
				break
			}
		}
		if exist {
			continue
		}

		rf.log = append(rf.log, entry)
	}

	// Rule 5
	if args.LeaderCommit > rf.commitIndex {
		dest := min(args.LeaderCommit, len(rf.log)-1)
		for i := rf.commitIndex; i <= dest; i++ {
			if i == 0 {
				continue
			}
			rf.Commit(i)
		}
		rf.commitIndex = dest
	}

	rf.pong = true

	reply.Term = rf.currentTerm
	reply.Success = true
	reply.NextIndex = rf.getLastLogIndex() + 1
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	defer rf.persist()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) CallRequestVote(server int) (bool, int) {
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}

	rf.mu.Lock()
	lastLogIndex := rf.getLastLogIndex()
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = lastLogIndex
	args.LastLogTerm = rf.log[lastLogIndex].Term
	rf.mu.Unlock()

	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
		return false, -1
	}

	return reply.VoteGranted, reply.Term
}

func (rf *Raft) CallAppendEntries(server int, entries []LogEntry) (bool, int, int) {
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}

	rf.mu.Lock()
	prevLogIndex := rf.nextIndex[server] - 1
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.Entries = entries
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = rf.log[prevLogIndex].Term
	args.LeaderCommit = rf.commitIndex
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		return false, -1, -1
	}

	return reply.Success, reply.Term, reply.NextIndex
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if !rf.isLeader() {
		isLeader = false
		return index, term, isLeader
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	index = 1
	lastLogIndex := rf.getLastLogIndex()
	for ; index <= lastLogIndex; index++ {
		log := rf.log[index]
		if log.Term == term && log.Command == command {
			DPrintf("[%v] cmd from client, index=%v, log=%+v\n", rf.me, index, log)
			return index, term, isLeader
		}
	}

	log := LogEntry{}
	log.Command = command
	log.Term = term

	rf.log = append(rf.log, log)
	DPrintf("[%v] cmd from client, index=%v, log=%+v\n", rf.me, index, log)

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	state := rf.state
	return state == Leader
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) ElectionTimer() {
	for !rf.killed() {
		interval := time.Millisecond * time.Duration(151+rand.Intn(300))
		time.Sleep(interval)

		if rf.isLeader() {
			continue
		}

		rf.mu.Lock()
		pong := rf.pong
		rf.mu.Unlock()
		if pong {
			rf.mu.Lock()
			rf.pong = false
			rf.mu.Unlock()
			continue
		}
		DPrintf("[%v] have not heard from leader or candidate for %v\n", rf.me, interval)

		// prepare a vote, vote for itself or others
		rf.mu.Lock()
		rf.votedFor = -1
		rf.mu.Unlock()
		// restart election timeout
		time.Sleep(interval)
		// before attempt an election, check again
		rf.mu.Lock()
		pong = rf.pong
		rf.mu.Unlock()
		if pong {
			continue
		}

		rf.AttemptElection()
	}
}

func (rf *Raft) AttemptElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	term := rf.currentTerm
	votes := 1
	votesDone := false
	rf.mu.Unlock()

	DPrintf("[%v] attempting an election at term %v\n", rf.me, term)

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			votedGranted, T := rf.CallRequestVote(server)
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if T > rf.currentTerm {
				DPrintf("[%v] request vote for %v but got higher term, quit election, T=%v\n", rf.me, server, T)
				rf.state = Follower
				rf.currentTerm = T
				rf.pong = true
				return
			}
			if !votedGranted {
				return
			}
			votes++
			DPrintf("[%v] got vote from %v\n", rf.me, server)
			if votesDone || votes <= len(rf.peers)/2 {
				return
			}
			votesDone = true
			if rf.state != Candidate || rf.currentTerm != term {
				return
			}
			DPrintf("[%v] got enough votes, become leader at term %v\n", rf.me, term)
			rf.BecomeLeader()
		}(server)
	}
}

func (rf *Raft) HeartBeat() {
	for !rf.killed() {
		if !rf.isLeader() {
			return
		}

		for server := range rf.peers {
			if server == rf.me {
				continue
			}

			go func(server int) {
				rf.sendHeartBeat(server)
				if !rf.isLeader() {
					return
				}
				rf.inspectCommit()
			}(server)
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (rf *Raft) sendHeartBeat(server int) {
	logEntries := make([]LogEntry, 0)
	rf.mu.Lock()
	lastLogIndex := rf.getLastLogIndex()
	nextIndex := rf.nextIndex[server]
	if lastLogIndex >= nextIndex {
		needAppend := rf.log[nextIndex:]
		logEntries = make([]LogEntry, len(needAppend))
		copy(logEntries, needAppend)
	}
	rf.mu.Unlock()

	ok, T, realNextIndex := rf.CallAppendEntries(server, logEntries)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if T > rf.currentTerm {
		DPrintf("[%v] append entries to %v but got higher term, T=%v\n", rf.me, server, T)
		rf.state = Follower
		rf.currentTerm = T
		rf.pong = true
		return
	}

	if ok {
		rf.nextIndex[server] = lastLogIndex + 1
		rf.matchIndex[server] = lastLogIndex
		DPrintf("[%v] successfully send entries to %v\n", rf.me, server)
	} else {
		DPrintf("[%v] send entries to %v failed, nextIndex=%v\n", rf.me, server, realNextIndex)
		if realNextIndex != -1 {
			rf.nextIndex[server] = realNextIndex
		} else {
			// server time out
			rf.nextIndex[server]--
		}
		if rf.nextIndex[server] <= 0 {
			rf.nextIndex[server] = 1
		}
	}
}

func (rf *Raft) inspectCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for n := rf.commitIndex + 1; n < len(rf.log); n++ {
		log := rf.log[n]
		// log from this term, commit by counting majority
		match := 1
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			if rf.matchIndex[server] >= n {
				match++
			}
			if match <= len(rf.peers)/2 {
				continue
			}
			// log from older term but not yet commit
			if log.Term != rf.currentTerm {
				DPrintf("[%v] an old log entry, log=%+v\n", rf.me, log)
			}
			DPrintf("[%v] majority match log, commit it, log=%+v\n", rf.me, log)
			rf.commitIndex = n
			rf.Commit(n)
			break
		}
	}
}

func (rf *Raft) Commit(index int) {
	log := rf.log[index]
	DPrintf("[%v] commit log entry, index=%v, log=%+v\n", rf.me, index, log)

	msg := ApplyMsg{}
	msg.CommandIndex = index
	msg.Command = log.Command
	msg.CommandValid = true
	rf.applyCh <- msg
}

func (rf *Raft) BecomeLeader() {
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	logLen := len(rf.log)
	for server := range rf.peers {
		rf.nextIndex[server] = logLen
		rf.matchIndex[server] = 0
	}
	go rf.HeartBeat()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	DPrintf("initialize %v\n", me)
	rf.state = Follower
	rf.pong = false
	rf.votedFor = -1
	rf.currentTerm = 0

	dummy := LogEntry{}
	dummy.Command = nil
	dummy.Term = -1
	rf.log = append(rf.log, dummy)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ElectionTimer()

	return rf
}
