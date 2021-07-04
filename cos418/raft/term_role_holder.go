package raft

import (
	"encoding/gob"
	"sync"
)

type TermUpgradeRequest struct {
	newTerm int
	signal  chan bool
}

// Ensures the term is updated to increase monotonically while proper
// components get notified. Also, raft role is coupled with the term number,
// it guarantees atomic update action to the term-role pair.
type TermRoleHolder struct {
	lock     sync.Mutex
	requests []*TermUpgradeRequest
	term     int
	role     RaftRole
}

func NewTermRoleHolder() *TermRoleHolder {
	th := new(TermRoleHolder)

	th.requests = make([]*TermUpgradeRequest, 0)
	th.term = 0
	th.role = RaftFollower

	return th
}

// Atomically reads the current term-role pair.
func (th *TermRoleHolder) CurrentTermRole() (int, RaftRole) {
	th.lock.Lock()
	defer th.lock.Unlock()

	return th.term, th.role
}

// Requests a partial update to the term and blocks, but only completes the
// update when another thread supplies the role with the term update. It will
// unblock when another thread accepts/rejects the term update.
func (th *TermRoleHolder) RequestTermUpgradeTo(newTerm int) {
	if newTerm <= th.term {
		return
	}

	th.lock.Lock()

	newRequest := new(TermUpgradeRequest)
	newRequest.newTerm = newTerm
	newRequest.signal = make(chan bool)

	th.requests = append(th.requests, newRequest)

	th.lock.Unlock()

	<-newRequest.signal
}

// Checks if there is any pending partial term update. If so, it retrieves one
// and tries to apply it. If the update makes a monotonic increase in term,
// then it takes the specified role and completes the update.
func (th *TermRoleHolder) ApplyPendingTermRoleUpgrade(
	roleIfUpgradable RaftRole,
) bool {
	th.lock.Lock()
	defer th.lock.Unlock()

	upgradable := false

	for len(th.requests) > 0 {
		// Apply all requests at once.
		newRequest := th.requests[0]
		th.requests = th.requests[1:]

		if newRequest.newTerm > th.term {
			th.term = newRequest.newTerm
			th.role = roleIfUpgradable
			upgradable = true
		}

		newRequest.signal <- true
	}

	return upgradable
}

// Updates the term-role pair if the term increases/keeps the current value.
func (th *TermRoleHolder) ApplyTermRoleUpgrade(
	newTerm int,
	roleIfUpgradable RaftRole,
) bool {
	th.lock.Lock()
	defer th.lock.Unlock()

	if newTerm < th.term {
		return false
	}

	th.term = newTerm
	th.role = roleIfUpgradable
	return true
}

func (th *TermRoleHolder) Serialize(encoder *gob.Encoder) {
	th.lock.Lock()
	defer th.lock.Unlock()

	encoder.Encode(th.term)
	encoder.Encode(th.role)
}

func (th *TermRoleHolder) Deserialize(decoder *gob.Decoder) {
	th.lock.Lock()
	defer th.lock.Unlock()

	decoder.Decode(&th.term)
	decoder.Decode(&th.role)
}
