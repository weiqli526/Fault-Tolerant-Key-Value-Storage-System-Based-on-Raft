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
	"fmt"
	"sync"
	"sync/atomic"

	"bytes"
	"cs651/labgob"
	"cs651/labrpc"
	"math/rand"
	"sort"
	"time"
)

// import "bytes"
// import "cs651/labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                   sync.Mutex          // Lock to protect shared access to this peer's state
	peers                []*labrpc.ClientEnd // RPC end points of all peers
	persister            *Persister          // Object to hold this peer's persisted state
	me                   int                 // this peer's index into peers[]
	dead                 int32               // set by Kill()
	applyCh              chan ApplyMsg       // apply a commited entry
	currentTerm          int                 // latest term server has seen
	votedFor             int                 // candidateId that received vote in current term
	logs                 []logEntryInfo      // log entries
	commitIndex          int                 // volatile state on all servers: index of highest log entry known to be committed
	lastApplied          int                 // volatile state on all servers: index of highest log entry applied to state machine
	nextIndex            []int               // volatile state on leaders: for each server, index of the next log entry to send to that server
	matchIndex           []int               // volatile state on leaders: for each server, index of highest log entry known to be replicated on server
	lastHeartbeat        time.Time           // time of reciving the previous heartbeat
	state                int                 // follower, candidate or leader
	timeoutPeriod        time.Duration       // timeout set in current term
	randGenerator        *rand.Rand          // to generate timeout in each term
	lastIncludeTerm      int                 // latest snapshot's lastIncludedTerm
	lastIncludeIndex     int                 // latest snapshot's lastIncludedIndex
	snapShot             []byte
	highestSnapShotIndex int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// each entry contains command for state machine,
// and term when entry was received by leader (first index is 1)
type logEntryInfo struct {
	Command interface{}
	Term    int
	Index   int
}

const (
	Candidate = 0
	Leader    = 1
	Follower  = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.SaveStateAndSnapshot() or use persister.SaveRaftState().
// after you've implemented snapshots, pass the current snapshot to persister.SaveStateAndSnapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []logEntryInfo
	var lastIncludedIndex int
	var lastIncludeTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludeTerm) != nil {
		fmt.Printf("Decode Failue\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs

		// 2D crash test
		rf.lastIncludeIndex = lastIncludedIndex
		rf.lastIncludeTerm = lastIncludeTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}
	// fmt.Printf("Restarting %v, logs %v", rf.me, len(rf.logs))
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	rf.mu.Lock()
	if index < rf.highestSnapShotIndex || index <= rf.lastIncludeIndex {
		rf.mu.Unlock()
		return
	}
	rf.highestSnapShotIndex = index

	if rf.lastIncludeIndex < index { //  && rf.lastApplied >= index ?
		rf.lastIncludeTerm = rf.logs[index-1-rf.lastIncludeIndex].Term
		prevIncludeIndex := rf.lastIncludeIndex
		rf.lastIncludeIndex = index
		rf.logs = rf.logs[index-prevIncludeIndex:]
	}

	// fmt.Printf("%v: Snapshot %v, index %v, lastApplied = %v, %v \n", rf.me, rf.lastIncludeIndex, index, rf.lastApplied, rf.lastIncludeIndex+len(rf.logs))

	// store each snapshot in the persister object
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	state := w.Bytes()
	rf.persister.SaveStateAndSnapshot(state, snapshot)

	rf.snapShot = snapshot

	rf.persist()

	rf.mu.Unlock()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// fmt.Printf("Receiving snapshot %v\n", args.Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
	} else {
		rf.mu.Lock()
		// fmt.Printf("%v calling InstallSnapshot to %v, args.lastIndex = %v, rf.lastIndex = %v, lastApplied = %v, commitIndex = %v\n", args.LeaderId, rf.me, args.LastIncludedIndex, rf.lastIncludeIndex, rf.lastApplied, rf.commitIndex)
		if args.LastIncludedIndex > rf.lastIncludeIndex {

			rf.lastHeartbeat = time.Now()
			if rf.state == Candidate {
				rf.state = Follower
			}
			if rf.currentTerm < args.Term {
				rf.votedFor = -1
				// fmt.Printf("%v updates term from %v", rf.me, rf.currentTerm)
				rf.updateTerm(args.Term)
				// fmt.Printf("to %v due to appendentries from %v\n", rf.currentTerm, args.LeaderId)
				if rf.state == Leader {
					rf.state = Follower
				}
			}

			// RPC not stale
			applymsg := ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      args.Data,
				SnapshotTerm:  args.LastIncludedTerm,
				SnapshotIndex: args.LastIncludedIndex,
			}

			rf.snapShot = args.Data

			if rf.lastIncludeIndex+len(rf.logs) > args.LastIncludedIndex {
				rf.logs = rf.logs[args.LastIncludedIndex-rf.lastIncludeIndex:]
			} else {
				rf.logs = []logEntryInfo{}
			}
			// rf.logs = []logEntryInfo{}
			rf.lastIncludeIndex = args.LastIncludedIndex
			rf.lastIncludeTerm = args.LastIncludedTerm
			/*if rf.commitIndex < args.LastIncludedIndex {
				rf.commitIndex = args.LastIncludedIndex
			}
			if rf.lastApplied < args.LastIncludedIndex {
				rf.lastApplied = args.LastIncludedIndex
			}*/
			rf.commitIndex = args.LastIncludedIndex
			rf.lastApplied = args.LastIncludedIndex

			rf.persist()
			rf.persister.SaveStateAndSnapshot(rf.persister.raftstate, args.Data)

			rf.mu.Unlock()

			rf.applyCh <- applymsg

		} else {
			rf.mu.Unlock()
		}
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // term currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("%d receives requestvote at term %d, from %d at term %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)

	if rf.currentTerm < args.Term || (rf.currentTerm == args.Term && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)) {

		// candidate's log is at least as up-to-date as receiver's log
		if len(rf.logs) == 0 && rf.lastIncludeIndex == 0 ||
			len(rf.logs) > 0 && (args.LastLogTerm > rf.logs[len(rf.logs)-1].Term || (args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= rf.logs[len(rf.logs)-1].Index)) ||
			len(rf.logs) == 0 && (args.LastLogTerm > rf.lastIncludeTerm || (args.LastLogTerm == rf.lastIncludeTerm && args.LastLogIndex >= rf.lastIncludeIndex)) {
			/*if len(rf.logs) > 0 {
				fmt.Printf("TRUE: %v, %v, %v, %v, Candidate: %v, Reciever: %v\n", args.LastLogTerm, rf.logs[len(rf.logs)-1].Term, args.LastLogIndex, len(rf.logs), args.CandidateId, rf.me)
			} else {
				fmt.Printf("TRUE: %v, %v, %v, Candidate: %v, Reciever: %v\n", args.LastLogTerm, args.LastLogIndex, len(rf.logs), args.CandidateId, rf.me)
			}*/

			reply.VoteGranted = true
			rf.votedFor = args.CandidateId

			// rf.updateTerm(args.Term)
			if rf.state == Leader {
				rf.state = Follower
			} else if rf.state == Candidate {
				rf.state = Follower
			}
			rf.lastHeartbeat = time.Now()
		} else {
			// fmt.Printf("FALSE: argsterm: %v, %v, argsIndex: %v, %v, Candidate: %v, Reciever: %v\n", args.LastLogTerm, rf.logs[len(rf.logs)-1].Term, args.LastLogIndex, rf.lastIncludeIndex+len(rf.logs), args.CandidateId, rf.me)
		}

		// rf.lastHeartbeat = time.Now()

		// update terms
		if rf.currentTerm < args.Term {
			// fmt.Printf("%v updates term from %v", rf.me, rf.currentTerm)
			rf.updateTerm(args.Term)
			// fmt.Printf("to %v due to voterequest from %v\n", rf.currentTerm, args.CandidateId)
			if rf.state == Leader {
				rf.state = Follower
			} else if rf.state == Candidate {
				rf.state = Follower
			}
		}

		rf.persist()

	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm

	// fmt.Printf("[%v, %v] %v replied %v to %v at (%v %v)\n", args.LastLogTerm, args.LastLogIndex, rf.me, reply.VoteGranted, args.CandidateId, args.Term, rf.currentTerm)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int            // leader's term
	LeaderId     int            // so follower can redirect clients
	PrevLogIndex int            // index of log entry immediately preceding new ones
	PrevLogTerm  int            // term of prevLogIndex entry
	Entries      []logEntryInfo // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int            // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	XTerm   int  // term in the conflicting entry (if any)
	XIndex  int  // index of first entry with that term (if any)
	XLen    int  // log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false

	if args.Term >= rf.currentTerm {
		// resets the elction timeout, convert states if necessery
		rf.lastHeartbeat = time.Now()
		if rf.state == Candidate {
			rf.state = Follower
		}
		if rf.currentTerm < args.Term {
			rf.votedFor = -1
			// fmt.Printf("%v updates term from %v", rf.me, rf.currentTerm)
			rf.updateTerm(args.Term)
			// fmt.Printf("to %v due to appendentries from %v\n", rf.currentTerm, args.LeaderId)
			if rf.state == Leader {
				rf.state = Follower
			}

		}

		// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		lastIndex := rf.lastIncludeIndex + len(rf.logs)

		// Otherwise
		if args.PrevLogIndex == 0 ||
			(args.PrevLogIndex > rf.lastIncludeIndex && args.PrevLogIndex-rf.lastIncludeIndex <= len(rf.logs) &&
				rf.logs[args.PrevLogIndex-1-rf.lastIncludeIndex].Term == args.PrevLogTerm) ||
			(args.PrevLogIndex <= rf.lastIncludeIndex) {
			reply.Success = true

			if args.Entries != nil {
				// If an existing entry conflicts with a new one (same index but different terms),
				// delete the existing entry and all that follow it
				// only trancate when a conflict exists as this may be an outdated RPC
				for idx, entry := range args.Entries {
					if entry.Index-1 == lastIndex {
						rf.logs = append(rf.logs, args.Entries[idx:]...)
						break
					}
					if entry.Index-rf.lastIncludeIndex >= 1 && rf.logs[entry.Index-1-rf.lastIncludeIndex].Term != entry.Term {
						rf.logs[entry.Index-1-rf.lastIncludeIndex] = entry
						rf.logs = append(rf.logs[:entry.Index-rf.lastIncludeIndex], args.Entries[idx+1:]...)
						break
					}
				}
			}

			// fmt.Printf("%v has entry length %v\n", rf.me, len(rf.logs))

			// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
			if rf.commitIndex < args.LeaderCommit {
				lastNewEntry := len(args.Entries) + args.PrevLogIndex
				if args.LeaderCommit < lastNewEntry {
					rf.commitIndex = args.LeaderCommit
				} else if rf.commitIndex < lastNewEntry {
					rf.commitIndex = lastNewEntry
				}
				go func() {
					rf.applyCommit()
				}()

			}

		} else {

			if lastIndex >= args.PrevLogIndex && args.PrevLogIndex > rf.lastIncludeIndex {
				reply.XTerm = rf.logs[args.PrevLogIndex-1-rf.lastIncludeIndex].Term
				for j := args.PrevLogIndex - 1 - rf.lastIncludeIndex; j >= 0 && rf.logs[j].Term == rf.logs[args.PrevLogIndex-1-rf.lastIncludeIndex].Term; j-- {
					reply.XIndex = j + 1
				}
				// fmt.Printf("Not %v: %v, %v, %v\n", args.PrevLogIndex, rf.logs[args.PrevLogIndex-1].Term, args.PrevLogTerm, len(rf.logs))
			} else if args.PrevLogIndex == rf.lastIncludeIndex {
				reply.XIndex = rf.lastIncludeIndex
				reply.XTerm = rf.lastIncludeTerm
			} else {
				reply.XLen = rf.lastIncludeIndex + len(rf.logs)
			}

			/*for i := range rf.logs {
				fmt.Printf("%v:%v ", rf.logs[i].Index, rf.logs[i].Term)
			}
			fmt.Printf("\n")*/
		}
		rf.persist()
	}
	reply.Term = rf.currentTerm
	// fmt.Printf("%d receives appendentry at term %d, from %d at term %d, Success: %v, args.prevIndex = %v, len %v, lastIncludeIndex %v\n", rf.me, rf.currentTerm, args.LeaderId, args.Term, reply.Success, args.PrevLogIndex, len(rf.logs), rf.lastIncludeIndex)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) updateTerm(term int) {
	rf.currentTerm = term
	randomInt := rf.randGenerator.Intn(500)
	rf.timeoutPeriod = time.Duration(randomInt+500) * time.Millisecond
}

// create a log entry and update it to its own log
func (rf *Raft) processCommand(command interface{}, index int, term int) {
	thisEntry := logEntryInfo{}
	thisEntry.Command = command
	thisEntry.Term = term
	thisEntry.Index = index

	/*if index > len(rf.logs) {
		rf.logs = append(rf.logs, make([]*logEntryInfo, index-len(rf.logs))...)
	}
	rf.logs[index-1] = thisEntry*/
	rf.logs = append(rf.logs, thisEntry)
	if rf.matchIndex[rf.me] < index {
		rf.matchIndex[rf.me] = index
	}

	rf.persist()

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
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).

	rf.mu.Lock()

	if rf.state != Leader {
		// if this server isn't the leader, returns false
		isLeader = false
		rf.mu.Unlock()
	} else if rf.killed() == false {
		isLeader = true
		// the first return value is the index that the command will appear at
		// if it's ever committed. the second return value is the current
		// term
		index = len(rf.logs) + 1 + rf.lastIncludeIndex
		term = rf.currentTerm

		// fmt.Printf("Client calls start to %v: idx %v, %v, term %v\n", rf.me, index, command, rf.currentTerm)

		// fmt.Printf("nextIndex = %v", rf.nextIndex)

		rf.nextIndex[rf.me] = index + 1

		defer func() {
			rf.processCommand(command, index, term)
			rf.mu.Unlock()
		}()
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// If election timeout elapses without receiving AppendEntries
		// RPC from current leader or granting vote to candidate
		rf.mu.Lock()
		timeoutPeriod := rf.timeoutPeriod
		rf.mu.Unlock()
		time.Sleep(timeoutPeriod / 3)
		rf.mu.Lock()
		if rf.state != Leader && time.Now().Sub(rf.lastHeartbeat) > rf.timeoutPeriod {
			go func() {
				rf.startElection()
			}()
			rf.mu.Unlock()
			time.Sleep(1500 * time.Millisecond)
		} else {
			// time.Sleep(300 * time.Millisecond)
			rf.mu.Unlock()
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	// Convert to candidate
	rf.mu.Lock()
	if rf.killed() != false {
		return
	}
	// fmt.Printf("Start electing for leader, %d at term %d\n", rf.me, rf.currentTerm)
	rf.state = Candidate

	votes := 0
	// Increment currentTerm
	rf.currentTerm += 1
	term := rf.currentTerm
	// Vote for self
	rf.votedFor = rf.me
	rf.persist()
	votes += 1

	total_rafts := len(rf.peers)
	idx := 0

	rf.persist()

	rf.mu.Unlock()

	var mutex sync.Mutex
	var wg sync.WaitGroup

	// args.LastLogIndex = 0
	rf.mu.Lock()
	me := rf.me

	lastLogIndex := len(rf.logs) + rf.lastIncludeIndex
	lastLogTerm := rf.lastIncludeTerm
	if len(rf.logs) != 0 {
		lastLogTerm = rf.logs[lastLogIndex-1-rf.lastIncludeIndex].Term
	}
	rf.mu.Unlock()

	for idx < total_rafts {
		// Send RequestVote RPCs to all other servers
		if idx != rf.me {
			wg.Add(1)
			go func(i int, term int, total_rafts int, me int, lastLogIndex int, lastLogTerm int) {
				defer wg.Done()

				args := RequestVoteArgs{}
				reply := RequestVoteReply{}
				args.Term = term
				args.CandidateId = me
				args.LastLogIndex = lastLogIndex
				args.LastLogTerm = lastLogTerm

				rf.peers[i].Call("Raft.RequestVote", &args, &reply)

				//  && time.Now().Sub(rf.lastHeartbeat) > rf.timeoutPeriod
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state == Candidate && rf.currentTerm == term {
					if reply.VoteGranted {

						mutex.Lock()
						votes += 1
						vote := votes
						mutex.Unlock()
						if vote > total_rafts/2 {
							// initialization
							rf.state = Leader
							peers_num := len(rf.peers)
							rf.matchIndex = make([]int, peers_num, peers_num)
							rf.nextIndex = make([]int, peers_num, peers_num)

							rf.matchIndex[rf.me] = rf.lastIncludeIndex + len(rf.logs)

							go func(term int) {
								rf.mu.Lock()
								// fmt.Printf("%d becomes leader of term %v, log len = %v, matchIndex = %v\n", rf.me, term, len(rf.logs), rf.matchIndex)
								// rf.actLeader(term)
								rf.mu.Unlock()
								rf.actLeader(term)
							}(term)
						}
						// fmt.Printf("%d receives vote from %d, now have vote %d. term %d\n", rf.me, i, vote, term)
					} else if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = Follower
						rf.persist()
					}
				}
			}(idx, term, total_rafts, me, lastLogIndex, lastLogTerm)
		}
		idx += 1
	}
	wg.Wait()
}

func (rf *Raft) actLeader(term int) {
	// Announce itself leader
	// Send heartbeat messages periodically
	rf.mu.Lock()
	for i := range rf.peers {
		if i != rf.me {
			rf.matchIndex[i] = 0
		}
	}
	for i := range rf.peers {
		// rf.nextIndex[i] = 1 + len(rf.logs)
		rf.nextIndex[i] = rf.lastIncludeIndex + len(rf.logs) + 1
	}
	rf.lastHeartbeat = time.Now()

	rf.mu.Unlock()

	for idx := range rf.peers {
		if idx != rf.me {
			go func(i int, term int) {
				rf.mu.Lock()
				isLeader := (rf.state == Leader) && rf.currentTerm == term
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
				for rf.killed() == false && isLeader {
					rf.mu.Lock()

					// fmt.Printf("600: %v, %v, %v, %v\n", rf.me, rf.nextIndex, len(rf.logs), rf.currentTerm)
					prevLogIndex := rf.nextIndex[i] - 1
					if prevLogIndex < rf.lastIncludeIndex {
						args := InstallSnapshotArgs{}
						reply := InstallSnapshotReply{}

						args.Term = rf.currentTerm
						args.LeaderId = rf.me
						args.LastIncludedIndex = rf.lastIncludeIndex
						args.LastIncludedTerm = rf.lastIncludeTerm
						args.Data = rf.persister.snapshot

						rf.nextIndex[i] = args.LastIncludedIndex + 1

						rf.mu.Unlock()

						rf.peers[i].Call("Raft.InstallSnapshot", &args, &reply)

						rf.mu.Lock()

						// fmt.Printf("Returning from installsnapshot to %v, replyTerm=%v, rf.currentTerm = %v\n", i, reply.Term, rf.currentTerm)

						if reply.Term > rf.currentTerm {
							rf.state = Follower
							rf.mu.Unlock()
							break
						}
						/*rf.matchIndex[i] = args.LastIncludedIndex
						fmt.Printf("matchIndex: %v\n", rf.matchIndex)*/
						rf.mu.Unlock()
					} else {
						args := AppendEntriesArgs{}
						reply := AppendEntriesReply{}

						args.Term = rf.currentTerm
						args.LeaderId = rf.me
						args.LeaderCommit = rf.commitIndex
						args.PrevLogIndex = prevLogIndex
						if args.PrevLogIndex > rf.lastIncludeIndex {
							if args.PrevLogIndex != 0 {
								args.PrevLogTerm = rf.logs[args.PrevLogIndex-1-rf.lastIncludeIndex].Term
							}

							// args.Entries = make([]*logEntryInfo, 0)

							if rf.nextIndex[i] < rf.nextIndex[rf.me] {
								// fmt.Printf("%v Sends log, from %v to %v, to %v, term = %v\n", rf.me, rf.nextIndex[i], rf.nextIndex[rf.me], i, args.Term)

								args.Entries = rf.logs[rf.nextIndex[i]-1-rf.lastIncludeIndex:]
								rf.nextIndex[i] += len(args.Entries)
							}
						} else if args.PrevLogIndex == rf.lastIncludeIndex {
							args.PrevLogTerm = rf.lastIncludeTerm
							if rf.nextIndex[i] < rf.nextIndex[rf.me] {
								// fmt.Printf("%v Sends log, from %v to %v, to %v, term = %v\n", rf.me, rf.nextIndex[i], rf.nextIndex[rf.me], i, args.Term)

								args.Entries = rf.logs[rf.nextIndex[i]-1-rf.lastIncludeIndex:]
								rf.nextIndex[i] += len(args.Entries)
							}
						}

						// fmt.Printf("i = %v: prevIdx, term = %v, %v\n", i, args.PrevLogIndex, args.PrevLogTerm)
						// fmt.Printf("615: %v, %v, %v, %v\n", rf.me, rf.nextIndex, len(rf.logs), rf.currentTerm)

						rf.mu.Unlock()

						rf.peers[i].Call("Raft.AppendEntries", &args, &reply)

						rf.mu.Lock()

						// fmt.Printf("Receives RPC, %v, %v, %v\n", reply.Success, args.PrevLogIndex, len(args.Entries))
						if rf.state == Leader && rf.currentTerm == term {
							// fmt.Printf("%v, %v: %v, %v, %v\n", rf.me, rf.currentTerm, i, rf.matchIndex, rf.nextIndex)
							if reply.Term > rf.currentTerm {
								rf.state = Follower
								rf.mu.Unlock()
								break
							} else if !reply.Success && rf.matchIndex[i] < args.PrevLogIndex && rf.currentTerm == term {
								// fmt.Printf("%v, %v, %v, len: %v, idx: %v, term: %v \n", rf.nextIndex, rf.matchIndex, args.PrevLogIndex, reply.XLen, reply.XIndex, reply.XTerm)
								if args.PrevLogIndex > 1 {
									if reply.XTerm != 0 {
										isTermExists := false
										if reply.XTerm <= rf.currentTerm {
											for j := 0; j < len(rf.logs); j++ {
												if rf.logs[j].Term == reply.XTerm {
													rf.nextIndex[i] = j + 1
													isTermExists = true
													break
												}
											}
										}
										if !isTermExists {
											rf.nextIndex[i] = reply.XIndex
										}
										index := sort.Search(len(rf.logs), func(j int) bool {
											return rf.logs[j].Term >= reply.XTerm
										})
										if index < len(rf.logs) && rf.logs[index].Term == reply.XTerm {
											rf.nextIndex[i] = index + rf.lastIncludeIndex
										} else {
											rf.nextIndex[i] = reply.XIndex
										}
									} else {
										rf.nextIndex[i] = reply.XLen + 1
									}
									// rf.nextIndex[i] = args.PrevLogIndex/2 + 1

									/*if rf.nextIndex[i] <= rf.matchIndex[i] {
										rf.nextIndex[i] = rf.matchIndex[i] + 1
									}*/
									// fmt.Printf("%v\n", rf.nextIndex)
								} else {
									rf.nextIndex[i] = 1
								}
								// fmt.Printf("RPC returns false, follower: %v, leader: %v, nextIndex %v: prevLog: %v, %v, %v, %v, %v\n", i, rf.me, rf.nextIndex, args.PrevLogIndex, reply.XIndex, reply.XTerm, reply.XLen, rf.matchIndex)
								/*for i := range rf.logs {
									fmt.Printf("%v:%v ", rf.logs[i].Index, rf.logs[i].Term)
								}
								fmt.Printf("\n")*/
								// fmt.Printf("654: %v, %v, %v, %v\n", rf.me, rf.nextIndex, len(rf.logs), rf.currentTerm)
							} else if reply.Success {
								if rf.matchIndex[i] < args.PrevLogIndex+len(args.Entries) {
									rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
									// fmt.Printf("%v updates matchIndex to %v, leader %v, term %v, %v, %v\n", i, rf.matchIndex[i], rf.me, rf.currentTerm, rf.nextIndex, rf.matchIndex)
								}
							} else {
								// fmt.Printf("Unsucess and no mismatch: %v\n", i)
							}
						}
						rf.mu.Unlock()
					}

					time.Sleep(3 * time.Millisecond)
					rf.mu.Lock()
					isLeader = (rf.state == Leader) && (rf.currentTerm == term)
					rf.mu.Unlock()
				}
			}(idx, term)
		}
	}

	go func(term int) {
		rf.checkCommitment(term)
	}(term)

}

func (rf *Raft) checkCommitment(term int) {
	rf.mu.Lock()
	isLeader := (rf.state == Leader) && rf.currentTerm == term
	rf.mu.Unlock()
	for isLeader {
		time.Sleep(1 * time.Millisecond)
		rf.mu.Lock()

		// check commitment status, from commitIndex to end
		if rf.currentTerm == term {
			for i := rf.nextIndex[rf.me] - 1; i > rf.commitIndex; i-- {
				nums_i := 0
				for j := range rf.matchIndex {
					if rf.matchIndex[j] >= i {
						nums_i += 1
					}
				}
				if rf.logs[i-1-rf.lastIncludeIndex].Term == rf.currentTerm && nums_i > len(rf.peers)/2 {
					// commit
					rf.commitIndex = i
					// fmt.Printf("Setting commitIndex to %v with %v followers, match_index = %v\n", i, nums_i, rf.matchIndex)
					break
				}
			}

			isLeader = (rf.state == Leader) && rf.currentTerm == term
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
			return
		}

	}
}

func (rf *Raft) applyEntry(i int) {
	rf.mu.Lock()
	applymsg := ApplyMsg{
		CommandValid: true,
		Command:      rf.logs[i-1-rf.lastIncludeIndex].Command,
		CommandIndex: i,
	}
	rf.mu.Unlock()
	// fmt.Printf("%v applied command, index %v\n", rf.me, applymsg.CommandIndex)
	rf.applyCh <- applymsg
}

func (rf *Raft) applyCommit() {
	rf.mu.Lock()
	// fmt.Printf("%v is applying index %v, %v, lastInclude %v\n", rf.me, rf.lastApplied, rf.commitIndex, rf.lastIncludeIndex)
	// applyMsgs := []ApplyMsg{}
	for i := rf.lastApplied + 1; i <= rf.commitIndex && !rf.killed(); i++ {
		/*applyMsgs = append(applyMsgs, ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i-1-rf.lastIncludeIndex].Command,
			CommandIndex: rf.logs[i-1-rf.lastIncludeIndex].Index,
		})*/
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i-1-rf.lastIncludeIndex].Command,
			CommandIndex: rf.logs[i-1-rf.lastIncludeIndex].Index,
			SnapshotTerm: rf.logs[i-1-rf.lastIncludeIndex].Term,
		}
		// fmt.Printf("%v: Index: %v, Command: %v\n", rf.me, rf.logs[i-1-rf.lastIncludeIndex].Index, rf.logs[i-1-rf.lastIncludeIndex].Command)
	}
	rf.lastApplied = rf.commitIndex
	rf.mu.Unlock()

	/*for _, applymsg := range applyMsgs {
		rf.applyCh <- applymsg
	}*/

	// fmt.Printf("Now %v has commit and applied index %v\n", rf.me, rf.commitIndex)
}

func (rf *Raft) checkApply() {
	for rf.killed() == false {
		rf.applyCommit()
		time.Sleep(1 * time.Millisecond)
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.logs = make([]logEntryInfo, 0)
	rf.votedFor = -1

	rf.state = Follower
	seed := time.Now().UnixNano() * int64(rf.me)
	rf.randGenerator = rand.New(rand.NewSource(seed))

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())
	// rf.lastApplied = len(rf.logs)
	// rf.commitIndex = rf.lastApplied

	// start ticker goroutine to start elections
	rf.lastHeartbeat = time.Now()
	rf.updateTerm(0)

	// for test code
	/*go func() {
		applymsg := ApplyMsg{
			CommandValid: true,
			Command:      0,
			CommandIndex: 0,
		}
		rf.applyCh <- applymsg
	}()*/
	rf.readPersist(rf.persister.raftstate)

	time.Sleep(50 * time.Millisecond)

	go rf.ticker()

	go rf.checkApply()

	return rf
}
