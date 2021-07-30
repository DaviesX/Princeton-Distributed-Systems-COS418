package raftkv

import (
	"cos418/cos418/raft"
	"sync"
)

//
type Op struct {
	Type  OpType
	Key   string
	Value string
}

func NewGetOp(key string) *Op {
	op := new(Op)
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

// A response that acts initially as a placeholder and could be fulfilled
// subsequently.
type FutureResponse struct {
	val    string
	signal chan bool
}

func NewFutureResponse() *FutureResponse {
	resp := new(FutureResponse)
	resp.signal = make(chan bool, 1)
	return resp
}

// Signals the client that hangs by the Wait() call to unblock.
func (fr *FutureResponse) Fulfill(ok bool) {
	fr.signal <- ok
}

// Waits until the future is fulfilled.
func (fr *FutureResponse) Wait() bool {
	return <-fr.signal
}

// Pools requests to allow them to be resolved in a different thread.
type RequestPool struct {
	mu          sync.Mutex
	futureResps map[int](*FutureResponse)
}

func NewRequestPool() *RequestPool {
	pool := new(RequestPool)
	pool.futureResps = make(map[int]*FutureResponse)
	return pool
}

// Adds a new request to the pool and returns a response future in the absence
// of failure. The user can use the future's Wait() call to wait until the
// resopnse is fulfilled. The doRequest function should send out a request and
// return a unique identifier associated with the request. If the returned
// identifier is less than zero, it will treat the request as failure, and
// therefore, returns a nil.
func (pool *RequestPool) AddRequest(
	doRequest func() int,
) *FutureResponse {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	id := doRequest()
	if id < 0 {
		return nil
	}

	resp := NewFutureResponse()
	pool.futureResps[id] = resp

	return resp
}

// Retrieves the response future by its id and removes it from the pool if it
// exists.
func (pool *RequestPool) PopAwaitingRequest(
	id int,
) *FutureResponse {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	resp := pool.futureResps[id]
	if resp != nil {
		delete(pool.futureResps, id)
	}

	return resp
}

// Pops an item from the operator queue and applies the operator to the kv
// store. In addition, it responds to previously enqueued requests from the
// request pool.
func ProcessOpQueue(
	queue chan raft.ApplyMsg,
	kvs *map[string]string,
	pool *RequestPool,
	shouldShutdown *bool,
) {
	for !*shouldShutdown {
		item := <-queue

		resp := pool.PopAwaitingRequest(item.Index)
		op := item.Command.(Op)

		ok := true
		switch op.Type {
		case OpPut:
			(*kvs)[op.Key] = op.Value
		case OpAppend:
			(*kvs)[op.Key] += op.Value
		case OpGet:
			if resp != nil {
				resp.val, ok = (*kvs)[op.Key]
			}
		}

		if resp != nil {
			resp.Fulfill(ok)
		}
	}
}
