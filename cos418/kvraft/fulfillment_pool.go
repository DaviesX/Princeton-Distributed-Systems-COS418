package raftkv

import (
	"sync"
)

// Represents a fulfillment task. It contains a response future where it gets
// fulfilled as the field fulfilled is set to true. The task's completion
// status is indicated by the field err.
type Fulfillment struct {
	future    *FutureResponse
	err       Err
	fulfilled bool
}

func NewFulfillment() *Fulfillment {
	fulfillment := new(Fulfillment)
	fulfillment.future = NewFutureResponse()
	fulfillment.err = ErrNone
	fulfillment.fulfilled = false
	return fulfillment
}

// Checks if the task is fulfilled.
func (f *Fulfillment) Fulfilled() bool {
	return f.fulfilled
}

// Keeps a collection of fulfillment tasks keyed by the corresponding ID of
// the caller which started the task. This ensures that a fulfillment applies
// to a specific call event exactly once. This data structure is thread safe.
type FulfillmentPool struct {
	mu           sync.Mutex
	fulfillments map[CallerId](*Fulfillment)
}

func NewFulfillmentPool() *FulfillmentPool {
	pool := new(FulfillmentPool)
	pool.fulfillments = make(map[CallerId](*Fulfillment))
	return pool
}

// Fetches the fulfillment task by its caller ID. A caller ID is the identity
// of the call event which started the fulfillment task. If the task has not
// been started by the caller before, a new task will be created from this
// point on.
func (pool *FulfillmentPool) Fetch(
	callerId CallerId,
) *Fulfillment {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	fulfillment, ok := pool.fulfillments[callerId]
	if !ok {
		placeholder := NewFulfillment()
		pool.fulfillments[callerId] = placeholder
		return placeholder
	}

	return fulfillment
}

// Checks if the fulfillment task associated with the call event had fulfilled.
func (pool *FulfillmentPool) Fulfilled(callerId CallerId) bool {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	fulfillment, ok := pool.fulfillments[callerId]
	return ok && fulfillment.Fulfilled()
}

// Makes the task associated with the call event fulfilled. Once a call event
//  is fulfilled, it can't be undone ever since.
func (pool *FulfillmentPool) Fulfill(
	callerId CallerId,
	err Err,
) {
	pool.mu.Lock()
	fulfillment := pool.fulfillments[callerId]
	fulfillment.err = err
	fulfillment.fulfilled = true
	pool.mu.Unlock()

	fulfillment.future.Fulfill(err)
}
