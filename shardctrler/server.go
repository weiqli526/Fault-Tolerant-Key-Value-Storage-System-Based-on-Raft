package shardctrler

import (
	"cs651/labgob"
	"cs651/labrpc"
	"cs651/raft"
	"fmt"
	"sort"
	"sync"
	"time"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	applyCommands map[int64]chan interface{}
	configs       []Config      // indexed by config num
	duplicate     map[int64]int // Client -> Seq
	lastQuery     map[int64]int
}

type Op struct {
	// Your data here.
	Type   string
	Args   interface{}
	Client int64
	Leader int
	Seq    int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	sc.mu.Lock()
	seq, exists := sc.duplicate[args.ClientId]
	if exists && seq >= args.Seq {
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		Type:   "Join",
		Args:   *args,
		Client: args.ClientId,
		Leader: sc.me,
		Seq:    args.Seq,
	}
	_, _, isLeader := sc.rf.Start(op)
	if isLeader {
		sc.mu.Lock()
		rpcChan := make(chan interface{}, 1)
		sc.applyCommands[args.ClientId] = rpcChan
		sc.mu.Unlock()
		select {
		case _ = <-sc.applyCommands[args.ClientId]:
			reply.WrongLeader = false
		case <-time.After(6000 * time.Millisecond):
			reply.Err = "TimeoutErr"
		}
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	seq, exists := sc.duplicate[args.ClientId]
	if exists && seq >= args.Seq {
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	op := Op{
		Type:   "Leave",
		Args:   *args,
		Client: args.ClientId,
		Leader: sc.me,
		Seq:    args.Seq,
	}
	_, _, isLeader := sc.rf.Start(op)
	if isLeader {
		sc.mu.Lock()
		rpcChan := make(chan interface{}, 1)
		sc.applyCommands[args.ClientId] = rpcChan
		sc.mu.Unlock()
		select {
		case _ = <-sc.applyCommands[args.ClientId]:
			reply.WrongLeader = false
		case <-time.After(6000 * time.Millisecond):
			reply.Err = "TimeoutErr"
		}
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	seq, exists := sc.duplicate[args.ClientId]
	if exists && seq >= args.Seq {
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	op := Op{
		Type:   "Move",
		Args:   *args,
		Client: args.ClientId,
		Leader: sc.me,
		Seq:    args.Seq,
	}
	_, _, isLeader := sc.rf.Start(op)
	if isLeader {
		sc.mu.Lock()
		rpcChan := make(chan interface{}, 1)
		sc.applyCommands[args.ClientId] = rpcChan
		sc.mu.Unlock()
		select {
		case _ = <-sc.applyCommands[args.ClientId]:
			reply.WrongLeader = false
		case <-time.After(6000 * time.Millisecond):
			reply.Err = "TimeoutErr"
		}
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	seq, exists := sc.duplicate[args.ClientId]
	if exists && seq >= args.Seq {
		reply.Config = sc.configs[int64(sc.lastQuery[args.ClientId])]
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		Type:   "Query",
		Args:   *args,
		Client: args.ClientId,
		Leader: sc.me,
		Seq:    args.Seq,
	}
	_, _, isLeader := sc.rf.Start(op)
	if isLeader {
		sc.mu.Lock()
		rpcChan := make(chan interface{}, 1)
		sc.applyCommands[args.ClientId] = rpcChan
		sc.mu.Unlock()
		select {
		case val := <-sc.applyCommands[args.ClientId]:
			reply.WrongLeader = false
			reply.Config = val.(Config)
		case <-time.After(6000 * time.Millisecond):
			reply.Err = "TimeoutErr"
		}
	} else {
		reply.WrongLeader = true
	}
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

	sc.duplicate = make(map[int64]int)
	sc.applyCommands = make(map[int64]chan interface{})
	sc.lastQuery = make(map[int64]int)

	go sc.applyTicker()

	return sc
}

func (sc *ShardCtrler) initNewConfig(config *Config) {
	config.Num = sc.configs[len(sc.configs)-1].Num + 1
	config.Shards = sc.configs[len(sc.configs)-1].Shards
	config.Groups = make(map[int][]string)

	for groupId, servers := range sc.configs[len(sc.configs)-1].Groups {
		config.Groups[groupId] = servers
	}
}

func (sc *ShardCtrler) applyTicker() {
	for {
		cmd := <-sc.applyCh
		if op, ok := cmd.Command.(Op); ok {
			sc.mu.Lock()
			seq, exist := sc.duplicate[op.Client]
			// fmt.Printf("Type: %v, seq %v, client %v, exist %v, seq %v, server %v, index %v\n", op.Type, op.Seq, op.Client, exist, seq, sc.me, cmd.CommandIndex)
			// fmt.Printf("%v, group %v, server %v\n", sc.configs[len(sc.configs)-1].Shards, len(sc.configs[len(sc.configs)-1].Groups), sc.me)
			if !exist || exist && seq < op.Seq {
				value := Config{}
				switch op.Type {
				case "Join":
					args := op.Args.(JoinArgs)
					// fmt.Printf("JOIN args: %v, server %v\n", args.Servers, sc.me)
					config := Config{}
					sc.initNewConfig(&config)

					newGroupNum := NShards / (len(args.Servers) + len(sc.configs[len(sc.configs)-1].Groups))
					if newGroupNum < 1 {
						newGroupNum = 1
					}

					var dets []int
					for dst := range args.Servers {
						dets = append(dets, dst)
					}
					sort.Ints(dets)

					if len(sc.configs[len(sc.configs)-1].Groups) == 0 {
						// The first JOIN
						shard := 0
						for _, dst := range dets {
							config.Groups[dst] = args.Servers[dst]
							// move newGroupNum shards to the new group
							for j := 0; j < newGroupNum; j++ {
								config.Shards[shard] = dst
								shard += 1
							}
							// NShards - 1 < shard - 1 + newGroupNum
							if NShards < shard+newGroupNum {
								for shard < NShards {
									config.Shards[shard] = dst
									shard += 1
								}
							}
						}
					} else {
						groupMap := getGroupGidMap(&sc.configs[len(sc.configs)-1])

						for _, dst := range dets {
							config.Groups[dst] = args.Servers[dst]
							// move newGroupNum shards to the new group
							for j := 0; j < newGroupNum; j++ {
								// Select a shard from the group that currently has the most shards
								src := getMaxShardGroup(groupMap)
								config.Shards[groupMap[src][0]] = dst
								groupMap[src] = groupMap[src][1:]
							}
						}
					}

					// fmt.Printf("%v, server %v\n", config, sc.me)

					sc.configs = append(sc.configs, config)

				case "Leave":
					args := op.Args.(LeaveArgs)
					// fmt.Printf("LEAVE args: %v, Server %v\n", args.GIDs, sc.me)
					config := Config{}
					sc.initNewConfig(&config)

					groupMap := getGroupGidMap(&sc.configs[len(sc.configs)-1])

					for _, gid := range args.GIDs {
						delete(config.Groups, gid)
						// move the shard it owns to the group that currently has the minimum shards
						shards := groupMap[gid]
						delete(groupMap, gid)
						for _, shard := range shards {
							dst := getMinShardGroup(groupMap)
							config.Shards[shard] = dst
							groupMap[dst] = append(groupMap[dst], shard)
						}
					}

					// fmt.Printf("%v, server %v\n", config, sc.me)
					sc.configs = append(sc.configs, config)

				case "Move":
					args := op.Args.(MoveArgs)
					config := Config{}
					sc.initNewConfig(&config)

					// fmt.Printf("MOVE args: gid %v: shard %v, server %v\n", args.GID, args.Shard, sc.me)

					config.Shards[args.Shard] = args.GID
					sc.configs = append(sc.configs, config)

					// fmt.Printf("%v, server %v\n", config, sc.me)

				case "Query":
					args := op.Args.(QueryArgs)
					if args.Num == -1 || args.Num > len(sc.configs)-1 {
						value = sc.configs[len(sc.configs)-1]
					} else {
						value = sc.configs[args.Num]
					}

					sc.lastQuery[args.ClientId] = value.Num
					// fmt.Printf("QueryArgs %v, seq %v, client %v, lastNum %v, ret %v, server %v\n", args.Num, args.Seq, args.ClientId, len(sc.configs)-1, value, sc.me)
				}
				sc.duplicate[op.Client] = op.Seq
				if op.Leader == sc.me {
					select {
					case sc.applyCommands[op.Client] <- value:
						break
					case <-time.After(200 * time.Millisecond):
						// fmt.Printf("chan no response\n")
						break
					}
				}
			}
			sc.mu.Unlock()

		} else {
			fmt.Println("Type assertion failed")
		}

	}
}

func getGroupGidMap(config *Config) map[int][]int {
	/* Get Map groupId -> pos in Shards */
	groupMap := make(map[int][]int)
	for groupId := range config.Groups {
		groupMap[groupId] = make([]int, 0)
	}
	for index, groupId := range config.Shards {
		groupMap[groupId] = append(groupMap[groupId], index)
	}
	return groupMap
}

func getMaxShardGroup(groupMap map[int][]int) int {
	max := -1
	maxId := 0
	for groupId, shards := range groupMap {
		if len(shards) > max || (len(shards) == max && groupId > maxId) { //
			maxId = groupId
			max = len(shards)
		}
	}
	return maxId
}

func getMinShardGroup(groupMap map[int][]int) int {
	min := NShards + 1
	minId := 0
	for groupId, shards := range groupMap {
		if len(shards) < min || (len(shards) == min && groupId > minId) { //
			minId = groupId
			min = len(shards)
		}
	}
	return minId
}
