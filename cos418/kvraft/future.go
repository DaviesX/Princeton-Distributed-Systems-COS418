package raftkv

import (
	"time"
)

// A response that acts initially as a placeholder and could be fulfilled
// subsequently. This object isn't thread safe.
type FutureResponse struct {
	val    string
	signal chan Err
}

func NewFutureResponse() *FutureResponse {
	future := new(FutureResponse)
	future.signal = make(chan Err, 2)
	return future
}

// Signals the client that hangs by the Wait() call to unblock.
func (fr *FutureResponse) Fulfill(err Err) {
	fr.signal <- err
}

func MakeFutureTimeout(timeout time.Duration, signal chan Err) {
	time.Sleep(timeout)
	signal <- ErrTimeout
}

// Waits until the future is fulfilled, or it gets timed out after 2 seoncds.
// In case of timeout, it returns ErrTimeout. Otherwise, it returns the error
// encountered during the future's fulfillment.
func (fr *FutureResponse) Wait() Err {
	go MakeFutureTimeout(2*time.Second, fr.signal)
	err := <-fr.signal
	return err
}
