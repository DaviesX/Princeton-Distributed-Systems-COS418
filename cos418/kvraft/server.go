package raftkv

import (
	"cos418/cos418/labrpc"
	"cos418/cos418/raft"
	"encoding/gob"
	"log"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type RaftKV struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	kvs             map[string]string
	fulfillmentPool *FulfillmentPool

	shouldShutdown bool
}

func RunOperation(
	callerId CallerId,
	rf *raft.Raft,
	fulfillmentPool *FulfillmentPool,
	buildOperationFn func(callerId CallerId) Op,
	reportWrongLeaderFn func(),
	processResponseFn func(*FutureResponse, Err),
) {
	fulfillment := fulfillmentPool.Fetch(callerId)
	if fulfillment.Fulfilled() {
		processResponseFn(fulfillment.future, fulfillment.err)
		return
	}

	op := buildOperationFn(callerId)
	_, _, isLeader := rf.Start(op)
	if !isLeader {
		reportWrongLeaderFn()
		return
	}

	err := fulfillment.future.Wait()

	processResponseFn(fulfillment.future, err)
}

// RPC to fetch the value from the key-value store associated with the key, if
// it exists in the KV store. The RPC will only succeed when the current node
// the service is running on has the leader status.
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	RunOperation(args.CID, kv.rf, kv.fulfillmentPool,
		func(callerId CallerId) Op {
			return NewGetOp(callerId, args.Key)
		},
		func() {
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
		}, func(resp *FutureResponse, err Err) {
			reply.Value = resp.val
			reply.WrongLeader = false
			reply.Err = err
		})
}

// RPC to insert the key-value pair into the key-value store or to append the
// specified value associated with the key, if it exists in the KV store,
// otherwise, a new key-value pair will be inserted.  The RPC will only succeed
// when the current node the service is running on has the leader status.
func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	RunOperation(args.CID, kv.rf, kv.fulfillmentPool,
		func(callerId CallerId) Op {
			return NewPutAppendOp(callerId, args.Op, args.Key, args.Value)
		},
		func() {
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
		}, func(resp *FutureResponse, err Err) {
			reply.WrongLeader = false
			reply.Err = err
		})
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	kv.shouldShutdown = true
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(
	servers []*labrpc.ClientEnd,
	me int,
	persister *raft.Persister,
	maxraftstate int,
) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvs = make(map[string]string)
	kv.fulfillmentPool = NewFulfillmentPool()

	kv.shouldShutdown = false

	go ProcessOpQueue(
		kv.me,
		kv.applyCh,
		&kv.kvs,
		kv.fulfillmentPool,
		&kv.shouldShutdown)

	return kv
}
