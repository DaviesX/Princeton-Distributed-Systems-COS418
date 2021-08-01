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
	Type  OpType
	Key   string
	Value string
}

func NewGetOp(key string) Op {
	var op Op
	op.Type = OpGet
	op.Key = key
	return op
}

func NewPutAppendOp(opType string, key string, value string) Op {
	var op Op
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
	if resp == nil {
		return
	}

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
	pool *RequestPool,
	shouldShutdown *bool,
) {
	fmt.Printf(
		"At node=%d: starting OperationQueueProcessor...\n", id)

	for !*shouldShutdown {
		item := <-queue
		op := item.Command.(Op)
		resp := pool.PopAwaitingRequest(item.Index)

		fmt.Printf("At node=%d: processing item=%v\n", id, item)

		err := ErrNone
		switch op.Type {
		case OpPut:
			(*kvs)[op.Key] = op.Value
		case OpAppend:
			(*kvs)[op.Key] += op.Value
		case OpGet:
			FetchValue(op.Key, *kvs, resp, &err)
		}

		if resp != nil {
			if item.Term == resp.term {
				resp.Fulfill(err)
			} else {
				resp.Fulfill(ErrOpOverwritten)
			}
		}
	}
}
