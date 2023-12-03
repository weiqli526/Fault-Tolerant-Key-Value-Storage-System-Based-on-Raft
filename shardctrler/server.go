package shardctrler

import (
	"cs651/labgob"
	"cs651/labrpc"
	"cs651/raft"
	"sync"
	"time"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	applyCommands map[int64]chan string
	configs       []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Type   string
	Args   interface{}
	Client int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Type:   "Join",
		Args:   *args,
		Client: args.ClientId,
	}
	_, _, isLeader := sc.rf.Start(op)
	if isLeader {
		sc.mu.Lock()
		rpcChan := make(chan string, 1)
		sc.applyCommands[args.ClientId] = rpcChan
		sc.mu.Unlock()
		select {
		case val := <-sc.applyCommands[args.ClientId]:
			reply.WrongLeader = false
			reply.Err = Err(val)
		case <-time.After(600 * time.Millisecond):
			reply.Err = "TimeoutErr"
		}
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Type:   "Leave",
		Args:   *args,
		Client: args.ClientId,
	}
	_, _, isLeader := sc.rf.Start(op)
	if isLeader {
		sc.mu.Lock()
		rpcChan := make(chan string, 1)
		sc.applyCommands[args.ClientId] = rpcChan
		sc.mu.Unlock()
		select {
		case val := <-sc.applyCommands[args.ClientId]:
			reply.WrongLeader = false
			reply.Err = Err(val)
		case <-time.After(600 * time.Millisecond):
			reply.Err = "TimeoutErr"
		}
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Type:   "Move",
		Args:   *args,
		Client: args.ClientId,
	}
	_, _, isLeader := sc.rf.Start(op)
	if isLeader {
		sc.mu.Lock()
		rpcChan := make(chan string, 1)
		sc.applyCommands[args.ClientId] = rpcChan
		sc.mu.Unlock()
		select {
		case val := <-sc.applyCommands[args.ClientId]:
			reply.WrongLeader = false
			reply.Err = Err(val)
		case <-time.After(600 * time.Millisecond):
			reply.Err = "TimeoutErr"
		}
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(QueryArgs{})
	labgob.Register(MoveArgs{})

	sc.applyTicker()

	return sc
}

func (sc *ShardCtrler) applyTicker() {
	for {
		cmd := <-sc.applyCh
		if op, ok := cmd.Command.(Op); ok {
			sc.mu.Lock()
			switch op.Type {
			case "Join":

			case "Leave":
			case "Move":
			case "Query":
			}
		}
	}
}
