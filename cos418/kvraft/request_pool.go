package raftkv

import (
	"cos418/cos418/raft"
	"sync"
)

// A response that acts initially as a placeholder and could be fulfilled
// subsequently.
type FutureResponse struct {
	term   raft.RaftTerm
	val    string
	signal chan Err
}

func NewFutureResponse(term raft.RaftTerm) *FutureResponse {
	resp := new(FutureResponse)
	resp.term = term
	resp.signal = make(chan Err, 1)
	return resp
}

// Signals the client that hangs by the Wait() call to unblock.
func (fr *FutureResponse) Fulfill(err Err) {
	fr.signal <- err
}

// Waits until the future is fulfilled.
func (fr *FutureResponse) Wait() Err {
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
	doRequest func() (int, raft.RaftTerm),
) *FutureResponse {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	id, term := doRequest()
	if id < 0 {
		return nil
	}

	resp := NewFutureResponse(term)
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
