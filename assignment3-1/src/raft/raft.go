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
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing the logEntries in Raft.
//
type logEntries struct {
	Term     int
	LogIndex int
	Command  interface{}
}

type State string

const (
	Leader    State = "Leader"
	Follower        = "Follower"
	Candidate       = "Candidate"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// State.
	serverState State

	// Persistent state on all servers:
	// Updated on stable storage before responding to RPCs.
	currentTerm int
	votedFor    int
	log         []*logEntries

	// Volatile state on all servers:
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	// Reinitialized after election.
	nextIndex  []int
	matchIndex []int

	// Auxiliary:
	electionTimeout int
	leaderTimeout   int
	votesCount      int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.serverState == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	CandidateTerm int
	candidateID   int
	lastLogIndex  int
	lastLogTerm   int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	candidateCurrentTerm int
	VoteGranted          bool
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	if args.CandidateTerm < rf.currentTerm {
		reply.VoteGranted = false
	} else {
		rf.serverState = Follower
		rf.currentTerm = args.CandidateTerm
		rf.electionTimeout = getElectionTimer()
		reply.VoteGranted = true
	}
	reply.candidateCurrentTerm = rf.currentTerm
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.currentTerm <= args.CurrentTerm {
		rf.serverState = Follower
		rf.currentTerm = args.CurrentTerm
		rf.electionTimeout = getElectionTimer()
	}
	reply.CurrentLeaderTerm = rf.currentTerm
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// Invoked by leader to replicate log entries.
// Also used as heartbeat.
//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	CurrentTerm       int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []*logEntries
	LeaderCommitIndex int
}

//
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	CurrentLeaderTerm int
	Success           bool
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.serverState = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]*logEntries, 0)
	rf.log = append(rf.log, &logEntries{0, 0, nil})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Main functionality for Raft leader election.
	go rf.StartElection()
	return rf
}

//
// StartElection should run in a separate goroutine. It continually checks whether no heartbeats have been received within the election timeout window.
// On receiving a heartbeat, it resets the election timeout. If none received, promotes to candidate state. If already a leader, sends heartbeats.
// Election process will be held if the server is not leader and no heartbeat has been received.
//
func (rf *Raft) StartElection() {
	rf.electionTimeout = getElectionTimer()
	oneMillisecond := 1 * time.Millisecond
	for {
		switch rf.serverState {
		case Follower:
			time.Sleep(oneMillisecond)
			rf.electionTimeout--
			if rf.electionTimeout < 0 {
				rf.serverState = Candidate
			}
		case Candidate:
			if rf.electionTimeout >= 0 {
				if rf.votesCount > int(len(rf.peers)/2) {
					rf.serverState = Leader
					rf.leaderTimeout = 0
				} else {
					time.Sleep(oneMillisecond)
					rf.electionTimeout--
				}
			} else {
				rf.electionTimeout = getElectionTimer()
				rf.currentTerm++
				rf.votesCount = 1
				for index := range rf.peers {
					if index != rf.me {
						go rf.StartVote(index)
					}
				}
			}
		case Leader:
			if rf.leaderTimeout >= 0 {
				time.Sleep(oneMillisecond)
				rf.leaderTimeout--
			} else {
				rf.leaderTimeout = 100
				for index := range rf.peers {
					if index != rf.me {
						go rf.ReplicateLog(index)
					}
				}
			}
		default:
			panic("Invalid state")
		}
	}
}

func (rf *Raft) StartVote(index int) {
	replies := RequestVoteReply{-1, false}
	lastElement := rf.log[len(rf.log)-1]
	rf.sendRequestVote(index, RequestVoteArgs{rf.currentTerm, rf.me, lastElement.LogIndex, lastElement.Term}, &replies)
	if replies.VoteGranted {
		rf.votesCount++
	} else {
		rf.currentTerm = replies.candidateCurrentTerm
		rf.serverState = Follower
	}
}

func (rf *Raft) ReplicateLog(index int) {
	replies := AppendEntriesReply{-1, false}
	rf.sendAppendEntries(index, AppendEntriesArgs{rf.currentTerm, rf.me, 0, 0, nil, 0}, &replies)
	if replies.CurrentLeaderTerm > rf.currentTerm {
		rf.currentTerm = replies.CurrentLeaderTerm
		rf.serverState = Follower
		rf.electionTimeout = getElectionTimer()
	}
}

func getElectionTimer() int {
	return 150 + rand.Intn(150)
}
