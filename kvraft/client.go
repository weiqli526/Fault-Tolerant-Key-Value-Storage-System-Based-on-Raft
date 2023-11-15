package kvraft

import (
	"crypto/rand"
	"cs651/labrpc"
	"fmt"
	"math/big"
	"time"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leader   int
	clientId int64
	// You will have to modify this struct.
	sequence int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	fmt.Printf("%v calls Get, ck.leader = %v\n", ck.clientId, ck.leader)
	// You will have to modify this function.
	ck.sequence += 1
	seq := ck.sequence
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		Seq:      seq,
	}
	reply := GetReply{}

	ok, val := func(k int, args GetArgs, reply GetReply) (bool, string) {
		fmt.Printf("%v Calling, server = %v\n", args.ClientId, k)
		ok := ck.servers[k].Call("KVServer.Get", &args, &reply)
		// fmt.Printf("%v Reply Success = %v, server = %v\n", args.ClientId, reply.Success, k)
		// time.Sleep(10 * time.Millisecond)
		if ok && reply.Success {
			return true, reply.Value
		}
		return false, ""
	}(ck.leader, args, reply)
	if !ok {
		j := 0
		for {
			time.Sleep(200 * time.Millisecond)
			ok, val = func(k int, args GetArgs, reply GetReply) (bool, string) {
				fmt.Printf("%v Calling, server = %v\n", args.ClientId, k)
				ok := ck.servers[k].Call("KVServer.Get", &args, &reply)
				// fmt.Printf("%v Reply Success = %v, server = %v\n", args.ClientId, reply.Success, k)
				// time.Sleep(10 * time.Millisecond)
				if ok && reply.Success {
					ck.leader = k
					return true, reply.Value
				}
				return false, ""
			}(j, args, reply)
			if ok {
				break
			} else {
				j = (j + 1) % (len(ck.servers))
			}
		}
	}
	return val
	/*retCh := make(chan string)

	go func(k int, args GetArgs, reply GetReply) {
		ok := ck.servers[k].Call("KVServer.Get", &args, &reply)
		// fmt.Printf("%v Reply Success = %v, server = %v\n", args.ClientId, reply.Success, k)
		// time.Sleep(10 * time.Millisecond)
		if ok && reply.Success {
			select {
			case retCh <- reply.Value:
				ck.leader = k
			default:
				fmt.Printf("channel closed\n")
			}
		}
	}(ck.leader, args, reply)

	select {
	case value := <-retCh:
		return value
	case <-time.After(250 * time.Millisecond):
		// j := 0
		for {
			// j = (j + 1) % len(ck.servers)
			for j := 0; j < len(ck.servers); j++ {
				go func(k int, args GetArgs, reply GetReply) {
					ok := ck.servers[k].Call("KVServer.Get", &args, &reply)
					// fmt.Printf("%v Reply Success = %v, server = %v\n", args.ClientId, reply.Success, k)
					// time.Sleep(10 * time.Millisecond)
					if ok && reply.Success {
						select {
						case retCh <- reply.Value:
							ck.leader = k
							// fmt.Printf("Setting leader to %v\n", k)
						default:
							fmt.Printf("channel closed\n")
						}

					}
				}(j, args, reply)
			}

			select {
			case value := <-retCh:
				close(retCh)
				return value
			case <-time.After(150 * time.Millisecond):
				continue
			}
		}
	}*/
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	fmt.Printf("%v calls Put, ck.leader = %v\n", ck.clientId, ck.leader)
	ck.sequence += 1
	seq := ck.sequence
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		Seq:      seq,
	}
	reply := PutAppendReply{}

	ok := func(k int, args PutAppendArgs, reply PutAppendReply) bool {
		fmt.Printf("%v Calling, server = %v\n", args.ClientId, k)
		ok := ck.servers[k].Call("KVServer.PutAppend", &args, &reply)
		// fmt.Printf("%v Reply Success = %v, server = %v\n", args.ClientId, reply.Success, k)
		// time.Sleep(10 * time.Millisecond)
		if ok && reply.Success {
			return true
		} else {
			return false
		}
	}(ck.leader, args, reply)

	if !ok {
		j := 0
		for {
			time.Sleep(200 * time.Millisecond)
			ok = func(k int, args PutAppendArgs, reply PutAppendReply) bool {
				fmt.Printf("%v Calling, server = %v\n", args.ClientId, k)
				ok := ck.servers[k].Call("KVServer.PutAppend", &args, &reply)
				// fmt.Printf("%v Reply Success = %v, server = %v\n", args.ClientId, reply.Success, k)
				// time.Sleep(10 * time.Millisecond)
				if ok && reply.Success {
					ck.leader = k
					return true
				} else {
					return false
				}
			}(j, args, reply)
			if ok {
				break
			} else {
				j = (j + 1) % (len(ck.servers))
			}
		}
	}

	/*retCh := make(chan bool)

	go func(k int, args PutAppendArgs, reply PutAppendReply) {
		ok := ck.servers[k].Call("KVServer.PutAppend", &args, &reply)
		// fmt.Printf("%v Reply Success = %v, server = %v\n", args.ClientId, reply.Success, k)
		// time.Sleep(10 * time.Millisecond)
		if ok && reply.Success {
			ck.leader = k
			select {
			case retCh <- reply.Success:
			default:
				fmt.Printf("channel closed\n")
			}
		}
	}(ck.leader, args, reply)

	select {
	case value := <-retCh:
		if value {
			return
		}
	case <-time.After(250 * time.Millisecond):
		for {
			for j := 0; j < len(ck.servers); j++ {
				go func(k int, args PutAppendArgs, reply PutAppendReply) {
					ok := ck.servers[k].Call("KVServer.PutAppend", &args, &reply)
					// fmt.Printf("%v Reply Success = %v, server = %v\n", args.ClientId, reply.Success, k)
					// time.Sleep(10 * time.Millisecond)
					if ok && reply.Success {
						ck.leader = k
						select {
						case retCh <- reply.Success:
							// fmt.Printf("Setting leader to %v\n", k)
						default:
							fmt.Printf("channel closed\n")
						}
					}
				}(j, args, reply)
			}

			select {
			case value := <-retCh:
				if value {
					return
				}

			case <-time.After(150 * time.Millisecond):
				continue
			}
		}
	}
	close(retCh)*/
	// fmt.Printf("set leader to %v, client %v\n", ck.leader, ck.clientId)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
