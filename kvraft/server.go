package kvraft

import (
	"cs651/labgob"
	"cs651/labrpc"
	"cs651/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key    string
	Type   string
	Value  string
	Client int64
	Seq    int
	Leader int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	duplicate     map[int64]*RPCInfo
	applyCommands map[int64]chan string
	dataDict      map[string]string
	highestIdx    int
	isLeader      bool
}

type RPCInfo struct {
	seq   int
	value string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	client := args.ClientId
	// kv.mu.Lock()
	value, exists := kv.duplicate[client]
	// kv.mu.Unlock()
	if !exists || value.seq < args.Seq {
		// Call Start
		op := Op{
			Key:    args.Key,
			Type:   "Get",
			Value:  "",
			Client: args.ClientId,
			Seq:    args.Seq,
			Leader: kv.me,
		}
		idx, _, isLeader := kv.rf.Start(op)
		kv.isLeader = isLeader
		fmt.Printf("isLeader %v, seq %v, clerk %v, server %v, index %v\n", isLeader, op.Seq, op.Client, kv.me, idx)
		if isLeader {
			// wait for idx to appear
			/*round := 0
			for round < 5 {
				time.Sleep(10 * time.Millisecond)
				kv.mu.Lock()
				value, exists := kv.duplicate[client]
				if exists && value.seq == op.Seq {
					kv.mu.Unlock()
					reply.Success = true
					reply.Value = value.value
					break
				} else if kv.highestIdx >= idx {
					kv.mu.Unlock()
					reply.Success = false
					break
				}
				kv.mu.Unlock()
				time.Sleep(50 * time.Millisecond)
				round += 1
			}*/
			// kv.mu.Lock()
			fmt.Printf("submit command with idx %v, seq %v, clerk %v, kv server %v\n", idx, op.Seq, op.Client, kv.me)
			_, exist := kv.applyCommands[client]
			if !exist {
				// fmt.Printf("Creating channel \n")
				kv.applyCommands[client] = make(chan string, 10)
			}
			// kv.mu.Unlock()

			select {
			case res := <-kv.applyCommands[client]:
				reply.Value = res
				reply.Success = true
				// close(kv.applyCommands[client])
				// delete(kv.applyCommands, client)
			case <-time.After(100 * time.Millisecond):
				reply.Success = false
				fmt.Printf("Replying false due to timeout %v, server %v, client %v\n", op.Seq, kv.me, op.Client)
			}

		} else {
			fmt.Printf("Server %v not leader\n", kv.me)
			reply.Success = false
			reply.Err = ErrWrongLeader
		}
	} else {
		fmt.Printf("Provessesed command %v:%v\n", args.ClientId, args.Seq)
		if kv.isLeader {
			reply.Value = value.value
			reply.Success = true
		}

	}

	fmt.Printf("Returnning seq %v, clerk %v, server %v\n", args.Seq, args.ClientId, kv.me)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	client := args.ClientId
	// kv.mu.Lock()
	value, exists := kv.duplicate[client]
	// kv.mu.Unlock()
	if !exists || value.seq < args.Seq {
		// Call Start
		op := Op{
			Key:    args.Key,
			Type:   args.Op,
			Value:  args.Value,
			Client: args.ClientId,
			Seq:    args.Seq,
			Leader: kv.me,
		}
		idx, _, isLeader := kv.rf.Start(op)
		kv.isLeader = isLeader
		fmt.Printf("isLeader %v, seq %v, clerk %v, server %v, index %v\n", isLeader, op.Seq, op.Client, kv.me, idx)
		if isLeader {
			// wait for idx to appear
			// fmt.Printf("submit command with idx %v, seq %v, clerk %v, kv server %v\n", idx, op.Seq, op.Client, kv.me)
			kv.mu.Lock()
			_, exist := kv.applyCommands[client]
			if !exist {
				kv.applyCommands[client] = make(chan string, 10)
				// fmt.Printf("Creating channel \n")
			}
			kv.mu.Unlock()
			select {
			case _ = <-kv.applyCommands[client]:
				reply.Success = true
				// close(kv.applyCommands[client])
				// delete(kv.applyCommands, client)
			case <-time.After(100 * time.Millisecond):
				fmt.Printf("Replying false due to timeout %v, server %v, client %v\n", op.Seq, kv.me, op.Client)
				reply.Success = false
			}
			/*round := 0
			for round < 5 {
				time.Sleep(10 * time.Millisecond)
				kv.mu.Lock()
				value, exists := kv.duplicate[client]
				if exists && value.seq == op.Seq {
					kv.mu.Unlock()
					reply.Success = true
					break
				} else if kv.highestIdx >= idx {
					kv.mu.Unlock()
					reply.Success = false
					break
				}
				kv.mu.Unlock()
				time.Sleep(50 * time.Millisecond)
				round += 1
			}*/

		} else {
			fmt.Printf("Server %v not leader\n", kv.me)
			reply.Err = ErrWrongLeader
			reply.Success = false
		}
	} else {
		if kv.isLeader {
			reply.Success = true
		}
	}

	fmt.Printf("Returnning seq %v, clerk %v, server %v\n", args.Seq, args.ClientId, kv.me)

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// fmt.Printf("Make kv server %v\n", me)
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.duplicate = make(map[int64]*RPCInfo)
	kv.dataDict = make(map[string]string)
	kv.applyCommands = make(map[int64]chan string)

	time.Sleep(50 * time.Millisecond)

	go kv.applyTicker()

	return kv
}

func (kv *KVServer) applyTicker() {
	// fmt.Printf("start ticker %v\n", kv.me)
	for {
		cmd := <-kv.applyCh
		if op, ok := cmd.Command.(Op); ok {
			kv.mu.Lock()
			value, exist := kv.duplicate[op.Client]
			if !exist || value.seq < op.Seq {
				val := ""
				switch op.Type {
				case "Put":
					kv.dataDict[op.Key] = op.Value
				case "Get":
					val, _ = kv.dataDict[op.Key]
				case "Append":
					val, _ = kv.dataDict[op.Key]
					kv.dataDict[op.Key] = val + op.Value
				}

				if op.Leader == kv.me {
					fmt.Printf("%v reply to channel, seq=%v, client=%v, index = %v\n", kv.me, cmd.CommandIndex, op.Seq, op.Client)
					kv.applyCommands[op.Client] <- val
				}

				// kv.duplicate[op.Client] = val
				kv.duplicate[op.Client] = &RPCInfo{seq: op.Seq, value: val}

				fmt.Printf("%v See cmd with idx %v, seq %v, clerk %v\n", kv.me, cmd.CommandIndex, op.Seq, op.Client)
			}

			kv.highestIdx = cmd.CommandIndex
			kv.mu.Unlock()
		} else {
			fmt.Println("Type assertion failed")
		}

	}
	// fmt.Printf("shut down ticker %v\n", kv.me)
}
