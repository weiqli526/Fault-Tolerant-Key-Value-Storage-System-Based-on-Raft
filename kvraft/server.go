package kvraft

import (
	"bytes"
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
	Term   int
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big

	// Your definitions here.
	duplicate     map[int64]RPCInfo
	applyCommands map[int64]chan string
	dataDict      map[string]string
	currentIdx    map[int64]int
	currentTerm   map[int64]int
	highestIdx    int
	isLeader      bool
	persister     *raft.Persister
}

type RPCInfo struct {
	Seq   int
	Value string
	Idx   int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// fmt.Printf("Get request %v, %v, server %v", args.ClientId, args.Seq, kv.me)

	client := args.ClientId
	kv.mu.Lock()
	value, exists := kv.duplicate[client]
	if !exists {
		kv.duplicate[client] = RPCInfo{}
	}
	// fmt.Printf("get 70, client %v, server %v\n", args.ClientId, kv.me)
	kv.mu.Unlock()
	if !exists || value.Seq < args.Seq {
		// Call Start
		op := Op{
			Key:    args.Key,
			Type:   "Get",
			Value:  "",
			Client: args.ClientId,
			Seq:    args.Seq,
			Leader: kv.me,
		}
		idx, term, isLeader := kv.rf.Start(op)
		// fmt.Printf("isLeader %v, seq %v, clerk %v, server %v, index %v\n", isLeader, op.Seq, op.Client, kv.me, idx)
		if isLeader {
			// wait for idx to appear
			kv.mu.Lock()
			/*duplicate_info := kv.duplicate[client]
			duplicate_info.Idx = idx
			kv.duplicate[client] = duplicate_info*/
			kv.currentIdx[client] = idx
			kv.currentTerm[client] = term
			// fmt.Printf("submit command with idx %v, seq %v, clerk %v, kv server %v\n", idx, op.Seq, op.Client, kv.me)
			rpcChan := make(chan string, 1)
			kv.applyCommands[client] = rpcChan
			kv.mu.Unlock()
			select {
			case val := <-kv.applyCommands[client]:
				// fmt.Printf("get value %v\n", val)
				reply.Success = true
				reply.Value = val
			case <-time.After(600 * time.Millisecond):
				kv.mu.Lock()
				reply.Success = false
				/*duplicate_info := kv.duplicate[client]
				duplicate_info.Idx = 0
				kv.duplicate[client] = duplicate_info*/
				kv.currentIdx[client] = 0
				kv.currentTerm[client] = -1
				// fmt.Printf("Replying false due to timeout %v, server %v, client %v\n", op.Seq, kv.me, op.Client)
				kv.mu.Unlock()
			}
			// close(rpcChan)
			// fmt.Printf("Replying false due to timeout %v, server %v, client %v\n", op.Seq, kv.me, op.Client)

		} else {
			// fmt.Printf("Server %v not leader\n", kv.me)
			reply.Success = false
			reply.Err = ErrWrongLeader
		}
	} else {
		// fmt.Printf("Processesed command %v:%v, server %v\n", args.ClientId, args.Seq, kv.me)
		reply.Success = true
		reply.Value = value.Value

	}

	// fmt.Printf("Returnning seq %v, clerk %v, server %v\n", args.Seq, args.ClientId, kv.me)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// fmt.Printf("Get request %v, %v, server %v", args.ClientId, args.Seq, kv.me)

	client := args.ClientId
	kv.mu.Lock()
	value, exists := kv.duplicate[client]
	if !exists {
		kv.duplicate[client] = RPCInfo{}
	}
	// fmt.Printf("get 70, client %v, server %v\n", args.ClientId, kv.me)
	kv.mu.Unlock()

	if !exists || value.Seq < args.Seq {
		// Call Start

		op := Op{
			Key:    args.Key,
			Type:   args.Op,
			Value:  args.Value,
			Client: args.ClientId,
			Seq:    args.Seq,
			Leader: kv.me,
		}
		idx, term, isLeader := kv.rf.Start(op)

		kv.isLeader = isLeader
		// fmt.Printf("isLeader %v, seq %v, clerk %v, server %v, index %v\n", isLeader, op.Seq, op.Client, kv.me, idx)
		if isLeader {
			// wait for idx to appear
			// fmt.Printf("submit command with idx %v, seq %v, clerk %v, kv server %v\n", idx, op.Seq, op.Client, kv.me)
			kv.mu.Lock()
			/*duplicate_info := kv.duplicate[client]
			duplicate_info.Idx = idx
			kv.duplicate[client] = duplicate_info*/
			kv.currentIdx[client] = idx
			kv.currentTerm[client] = term
			rpcChan := make(chan string, 1)
			kv.applyCommands[client] = rpcChan
			kv.mu.Unlock()
			select {
			case _ = <-kv.applyCommands[client]:
				reply.Success = true
			case <-time.After(600 * time.Millisecond):
				kv.mu.Lock()
				reply.Success = false
				/*duplicate_info := kv.duplicate[client]
				duplicate_info.Idx = 0
				kv.duplicate[client] = duplicate_info*/
				kv.currentIdx[client] = 0
				kv.currentTerm[client] = -1
				// fmt.Printf("Replying false due to timeout %v, server %v, client %v\n", op.Seq, kv.me, op.Client)
				kv.mu.Unlock()
			}
			// close(rpcChan)

		} else {
			// fmt.Printf("Server %v not leader\n", kv.me)
			reply.Err = ErrWrongLeader
			reply.Success = false
		}
	} else {
		// fmt.Printf("Processesed command %v:%v, server %v\n", args.ClientId, args.Seq, kv.me)
		reply.Success = true
	}

	// fmt.Printf("Returnning seq %v, clerk %v, server %v\n", args.Seq, args.ClientId, kv.me)

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
	kv.duplicate = make(map[int64]RPCInfo)
	kv.dataDict = make(map[string]string)
	kv.applyCommands = make(map[int64]chan string)
	kv.currentIdx = make(map[int64]int)
	kv.currentTerm = make(map[int64]int)

	kv.persister = persister

	kv.mu.Lock()
	kv.readPersist(persister.ReadSnapshot())
	kv.mu.Unlock()
	// kv.maxraftstate = -1

	// time.Sleep(50 * time.Millisecond)

	go kv.applyTicker()

	/*if maxraftstate != -1 {
		go kv.performSnapshot()
	}*/

	return kv
}

func (kv *KVServer) readPersist(data []byte) {
	// read from snapshot
	// dataDict and duplicate table
	// fmt.Printf("%v readPersist\n", kv.me)
	// kv.mu.Lock()
	if data != nil && len(data) >= 1 {
		r := bytes.NewBuffer(data)
		d := labgob.NewDecoder(r)
		dataDict := make(map[string]string)
		duplicate := make(map[int64]RPCInfo)
		var highestIdx int
		if d.Decode(&dataDict) != nil || d.Decode(&duplicate) != nil || d.Decode(&highestIdx) != nil {
			fmt.Printf("%v Decode Failue\n", kv.me)
		}
		kv.dataDict = dataDict
		kv.duplicate = duplicate
		kv.applyCommands = make(map[int64]chan string)
		kv.currentIdx = make(map[int64]int)
		kv.currentTerm = make(map[int64]int)
		kv.highestIdx = highestIdx
	}
	// kv.mu.Unlock()
}

func (kv *KVServer) applyTicker() {
	// fmt.Printf("start ticker %v\n", kv.me)
	for kv.killed() == false {
		cmd := <-kv.applyCh
		if cmd.SnapshotValid {
			kv.mu.Lock()
			if cmd.SnapshotIndex > kv.highestIdx {
				// fmt.Printf("%v install snapshot, snapshot idx %v, highest idx %v\n", kv.me, cmd.SnapshotIndex, kv.highestIdx)
				kv.readPersist(cmd.Snapshot)
				// kv.highestIdx = cmd.SnapshotIndex
				if cmd.SnapshotIndex != kv.highestIdx {
					fmt.Printf("\n Install Snapshot server %v cmd %v: kv %v\n", kv.me, cmd.SnapshotIndex, kv.highestIdx)
				}
			}
			kv.mu.Unlock()
		} else {
			if op, ok := cmd.Command.(Op); ok {
				kv.mu.Lock()
				if cmd.CommandIndex > kv.highestIdx {
					if cmd.CommandIndex != kv.highestIdx+1 {
						fmt.Printf("\n server %v cmd %v: kv %v\n", kv.me, cmd.CommandIndex, kv.highestIdx)
					}
					value, exist := kv.duplicate[op.Client]
					if (!exist && op.Seq == 1) || (exist && value.Seq == op.Seq-1) {
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

						if !exist {
							kv.duplicate[op.Client] = RPCInfo{Seq: op.Seq, Value: val, Idx: 0}
						} else {
							duplicate_info := kv.duplicate[op.Client]
							duplicate_info.Seq = op.Seq
							duplicate_info.Value = val
							kv.duplicate[op.Client] = duplicate_info
						}

						// kv.duplicate[op.Client] = val
						//

						if op.Leader == kv.me && kv.currentIdx[op.Client] == cmd.CommandIndex && kv.currentTerm[op.Client] == cmd.SnapshotTerm && kv.duplicate[op.Client].Seq == op.Seq && cmd.CommandIndex != 0 {

							/*duplicate_info := kv.duplicate[op.Client]
							duplicate_info.Idx = 0
							kv.duplicate[op.Client] = duplicate_info*/
							kv.currentIdx[op.Client] = 0

							// fmt.Printf("%v reply to channel, index = %v, seq=%v, client=%v\n", kv.me, cmd.CommandIndex, op.Seq, op.Client)
							select {
							case kv.applyCommands[op.Client] <- val:
								break
							case <-time.After(200 * time.Millisecond):
								break
							}
						}
						// fmt.Printf("%v See cmd with idx %v, seq %v, clerk %v\n", kv.me, cmd.CommandIndex, op.Seq, op.Client)
					} else {
						// fmt.Printf("op %v: Value %v", op.Seq, value.Seq)
					}
					kv.highestIdx = cmd.CommandIndex
					if kv.maxraftstate != -1 {
						kv.performSnapshot(cmd.CommandIndex)
					}

				}
				kv.mu.Unlock()
			} else {
				fmt.Println("Type assertion failed")
			}

		}
	}
	// fmt.Printf("shut down ticker %v\n", kv.me)
}

func (kv *KVServer) performSnapshot(index int) {
	/* Detects when the persisted Raft state grows too large,
	and then hands a snapshot to Raft. When a kvserver server
	restarts, it should read the snapshot from persister and
	restore its state from the snapshot. */
	// fmt.Printf("%v performs snapshot, to index %v\n", kv.me, index)
	if kv.persister.RaftStateSize() >= 4*kv.maxraftstate {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.dataDict)
		e.Encode(kv.duplicate)
		e.Encode(index)
		data := w.Bytes()
		kv.rf.Snapshot(index, data)
	}
}
