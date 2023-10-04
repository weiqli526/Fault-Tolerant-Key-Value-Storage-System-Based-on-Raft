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
	"sync"
	"sync/atomic"

	"cs651/labrpc"
	"math/rand"
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
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	dead          int32               // set by Kill()
	currentTerm   int                 // latest term server has seen
	votedFor      int                 // candidateId that received vote in current term
	logs          []*logEntryInfo     // log entries
	commitIndex   int                 // index of highest log entry known to be committed
	lastApplied   int                 // index of highest log entry applied to state machine
	nextIndex     []int               // for each server, index of the next log entry to send to that server
	matchIndex    []int               // for each server, index of highest log entry known to be replicated on server
	lastHeartbeat time.Time
	state         int
	timeoutPeriod time.Duration
	randGenerator *rand.Rand

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// leach entry contains command for state machine,
// and term when entry was received by leader (first index is 1)
type logEntryInfo struct {
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
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

	if rf.currentTerm < args.Term || (rf.currentTerm == args.Term && rf.votedFor == -1) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term

		if rf.state == Leader {
			rf.nextIndex = nil
			rf.matchIndex = nil
			rf.state = Follower
		} else if rf.state == Candidate {
			rf.state = Follower
		}
		rf.lastHeartbeat = time.Now()
		if rf.currentTerm < args.Term {
			rf.updateTerm(args.Term)
		}
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
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
	Term         int   // leader's term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []int // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("%d receives appendentry at term %d, from %d at term %d\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	if args.Entries == nil {
		// empty heartbeat msgs
		if args.Term < rf.currentTerm {
			reply.Success = false
		} else {
			// resets the elction timeout
			rf.lastHeartbeat = time.Now()
			if rf.state == Candidate {
				rf.state = Follower
			}
			if rf.currentTerm < args.Term {
				rf.votedFor = -1
				rf.updateTerm(args.Term)
				if rf.state == Leader {
					rf.nextIndex = nil
					rf.matchIndex = nil
					rf.state = Follower
				}
			}
		}
		reply.Term = rf.currentTerm
	} else {

	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) updateTerm(term int) {
	rf.currentTerm = term

	randomInt := rf.randGenerator.Intn(250)
	rf.timeoutPeriod = time.Duration(randomInt+300) * time.Millisecond
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
	isLeader := true

	// Your code here (2B).

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

		time.Sleep(time.Millisecond * time.Duration(5*rf.me%5+50+rf.me%50))

		// If election timeout elapses without receiving AppendEntries
		// RPC from current leader or granting vote to candidate
		rf.mu.Lock()
		if rf.state != Leader && time.Now().Sub(rf.lastHeartbeat) > rf.timeoutPeriod {
			go func() {
				// fmt.Printf("Start electing for leader, %d at term %d\n", rf.me, rf.currentTerm)
				rf.startElection()
			}()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	// Convert to candidate
	rf.mu.Lock()
	rf.state = Candidate

	votes := 0
	// Increment currentTerm
	rf.currentTerm += 1
	term := rf.currentTerm
	// Vote for self
	rf.votedFor = rf.me
	votes += 1

	total_rafts := len(rf.peers)
	idx := 0

	rf.mu.Unlock()

	var mutex sync.Mutex
	var wg sync.WaitGroup

	for idx < total_rafts {
		// Send RequestVote RPCs to all other servers
		if idx != rf.me {
			wg.Add(1)
			go func(i int, term int, total_rafts int) {
				defer wg.Done()

				mutex.Lock()
				defer mutex.Unlock()

				args := RequestVoteArgs{}
				reply := RequestVoteReply{}
				args.Term = term
				args.CandidateId = rf.me

				rf.peers[i].Call("Raft.RequestVote", &args, &reply)

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.currentTerm == term && time.Now().Sub(rf.lastHeartbeat) > rf.timeoutPeriod {
					if reply.VoteGranted {
						// fmt.Printf("%d receives vote from %d, now have vote %d. term %d\n", rf.me, i, votes, term)
						votes += 1
						if votes > total_rafts/2 {
							rf.state = Leader
							go func() {
								// fmt.Printf("%d becomes leader \n", rf.me)
								rf.actLeader()
							}()
						}
					}
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
				}
			}(idx, term, total_rafts)
		}
		idx += 1
	}
	wg.Wait()
}

func (rf *Raft) actLeader() {
	// Announce itself leader
	// Send heartbeat messages periodically

	for idx := range rf.peers {
		if idx != rf.me {
			go func(i int) {
				rf.mu.Lock()
				isLeader := (rf.state == Leader)
				rf.mu.Unlock()
				for isLeader {
					rf.mu.Lock()
					args := AppendEntriesArgs{}
					reply := AppendEntriesReply{}
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					rf.mu.Unlock()

					rf.peers[i].Call("Raft.AppendEntries", &args, &reply)

					time.Sleep(150 * time.Millisecond)
					rf.mu.Lock()
					isLeader = (rf.state == Leader)
					rf.mu.Unlock()
				}
			}(idx)
		}
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
	rf.logs = nil
	rf.votedFor = -1
	rf.currentTerm = 0

	rf.state = Follower
	seed := time.Now().UnixNano() * int64(rf.me)
	rf.randGenerator = rand.New(rand.NewSource(seed))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.lastHeartbeat = time.Now()
	rf.updateTerm(0)

	go rf.ticker()

	return rf
}
