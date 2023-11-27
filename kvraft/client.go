package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"cs651/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	// clerk unique id
	ckId int64
	// request seq id
	seqId int
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
	ck.ckId = nrand()
	ck.seqId = 0
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
	// You will have to modify this function.
	ck.seqId += 1
	args := GetArgs{
		Key:      key,
		ClientId: ck.ckId,
		Seq:      ck.seqId,
	}

	reply := GetReply{}
	server := ck.leaderId % len(ck.servers)
	for {
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if ok && reply.Success == true {
			ck.leaderId = server
			break
		}
		server = (server + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}

	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
// OK             = "OK"
// ErrNoKey       = "ErrNoKey"
// ErrWrongLeader = "ErrWrongLeader"

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqId += 1
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.ckId,
		Seq:      ck.seqId,
	}
	reply := PutAppendReply{}
	server := ck.leaderId % len(ck.servers)
	for {
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Success == true {
			ck.leaderId = server
			break
		}
		server = (server + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}

	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
