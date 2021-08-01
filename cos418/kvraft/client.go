package raftkv

import (
	"cos418/cos418/labrpc"
	"crypto/rand"
	"fmt"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	// Current known leader.
	leaderServer KvNodeId
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
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	for {
		var args GetArgs
		args.Key = key

		var reply GetReply

		fmt.Printf("To=%d: GetArgs=%v\n", ck.leaderServer, args)
		ok := ck.servers[ck.leaderServer].Call("RaftKV.Get", &args, &reply)

		if !ok || reply.WrongLeader || reply.Err == ErrOpOverwritten {
			ck.leaderServer = (ck.leaderServer + 1) % len(ck.servers)
			continue
		}

		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	for {
		var args PutAppendArgs
		args.Op = op
		args.Key = key
		args.Value = value

		var reply PutAppendReply
		fmt.Printf("To=%d: PutAppendArgs=%v\n", ck.leaderServer, args)
		ok := ck.servers[ck.leaderServer].Call("RaftKV.PutAppend", &args, &reply)

		if !ok || reply.WrongLeader || reply.Err == ErrOpOverwritten {
			ck.leaderServer = (ck.leaderServer + 1) % len(ck.servers)
			continue
		}

		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
