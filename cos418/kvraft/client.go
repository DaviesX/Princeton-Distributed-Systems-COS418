package raftkv

import (
	"cos418/cos418/labrpc"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	// Current known leader.
	leaderServer KvNodeId
}

func GenerateCallerId() CallerId {
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

func (ck *Clerk) NextLeader() {
	ck.leaderServer = (ck.leaderServer + 1) % len(ck.servers)
}

func (ck *Clerk) RunCall(
	procedureName string,
	buildArgsFn func(callerId CallerId) interface{},
	createReplyPtrFn func() interface{},
	processReplyFn func(interface{}) (string, Err),
) string {
	thisCallerId := GenerateCallerId()

	for {
		args := buildArgsFn(thisCallerId)
		reply := createReplyPtrFn()

		ok := ck.servers[ck.leaderServer].Call(procedureName, args, reply)

		result, err := processReplyFn(reply)

		if !ok || err == ErrWrongLeader {
			ck.NextLeader()
			continue
		}

		if err == ErrTimeout {
			continue
		}

		return result
	}
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
	return ck.RunCall("RaftKV.Get",
		func(callerId CallerId) interface{} {
			args := new(GetArgs)
			args.CID = callerId
			args.Key = key
			return args
		},
		func() interface{} {
			return new(GetReply)
		},
		func(reply interface{}) (string, Err) {
			return reply.(*GetReply).Value, reply.(*GetReply).Err
		})
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
	ck.RunCall("RaftKV.PutAppend",
		func(callerId CallerId) interface{} {
			args := new(PutAppendArgs)
			args.CID = callerId
			args.Op = op
			args.Key = key
			args.Value = value
			return args
		},
		func() interface{} {
			return new(PutAppendReply)
		},
		func(reply interface{}) (string, Err) {
			return "", reply.(*PutAppendReply).Err
		})
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
