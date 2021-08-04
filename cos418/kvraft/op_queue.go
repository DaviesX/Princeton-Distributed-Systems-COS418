package raftkv

import (
	"cos418/cos418/raft"
	"fmt"
)

// An operation entry consists of an operator type, key and value operand. This
// is used as a raft log entry. Raft will keep track of the operation sequence
// in a consistent manner so that every KV store replica will agree on the
// state.
type Op struct {
	CID   CallerId
	Type  OpType
	Key   string
	Value string
}

func NewGetOp(callerId CallerId, key string) Op {
	var op Op
	op.CID = callerId
	op.Type = OpGet
	op.Key = key
	return op
}

func NewPutAppendOp(
	callerId CallerId,
	opType string,
	key string,
	value string,
) Op {
	var op Op
	op.CID = callerId
	if opType == "Put" {
		op.Type = OpPut
	} else if opType == "Append" {
		op.Type = OpAppend
	} else {
		panic("bad op type.")
	}
	op.Key = key
	op.Value = value
	return op
}

func FetchValue(
	key string,
	kvs map[string]string,
	resp *FutureResponse,
	err *Err,
) {
	val, ok := kvs[key]
	if !ok {
		*err = ErrNoSuchKey
		return
	}

	resp.val = val
}

// Pops an item from the operator queue and applies the operator to the kv
// store. In addition, it responds to previously enqueued requests from the
// request pool.
func ProcessOpQueue(
	id KvNodeId,
	queue chan raft.ApplyMsg,
	kvs *map[string]string,
	fulfillmentPool *FulfillmentPool,
	shouldShutdown *bool,
) {
	fmt.Printf(
		"At node=%d: starting the operation queue processor...\n", id)

	for !*shouldShutdown {
		item := <-queue
		op := item.Command.(Op)

		if fulfillmentPool.Fulfilled(op.CID) {
			// Skip a duplicate operation.
			continue
		}

		fmt.Printf("At node=%d: processing item=%v\n", id, item)

		future := fulfillmentPool.Fetch(op.CID).future

		err := ErrNone
		switch op.Type {
		case OpPut:
			(*kvs)[op.Key] = op.Value
		case OpAppend:
			(*kvs)[op.Key] += op.Value
		case OpGet:
			FetchValue(op.Key, *kvs, future, &err)
		}

		fulfillmentPool.Fulfill(op.CID, err)
	}

	fmt.Printf(
		"At node=%d: stopping the operation queue processor...\n", id)
}
