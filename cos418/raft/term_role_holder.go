package raft

import (
	"encoding/gob"
	"fmt"
	"sync"
)

type RaftTerm int
type RaftRole string

const NilTerm RaftTerm = -1

const (
	RaftFollower  RaftRole = "RaftFollower"
	RaftCandidate RaftRole = "RaftCandidate"
	RaftLeader    RaftRole = "RaftLeader"
)

// Ensures the term is updated to increase monotonically while proper
// components get notified. Also, raft role is coupled with the term number,
// it guarantees atomic update action to the term-role pair.
type TermRoleHolder struct {
	lock sync.Mutex
	term RaftTerm
	role RaftRole
}

func NewTermRoleHolder() *TermRoleHolder {
	th := new(TermRoleHolder)

	th.term = 0
	th.role = RaftFollower

	return th
}

// Atomically reads the current term-role pair.
func (th *TermRoleHolder) CurrentTermRole() (RaftTerm, RaftRole) {
	th.lock.Lock()
	defer th.lock.Unlock()

	return th.term, th.role
}

type TermUpgradeReason string

const (
	TURLackOfHeartbeat             TermUpgradeReason = "LackOfHeartbeat"
	TURSplitVotes                  TermUpgradeReason = "SplitVotes"
	TURWonAnElection               TermUpgradeReason = "WonAnElection"
	TUREncouteredHigherTermMessage TermUpgradeReason = "EncounteredHigherTermMessage"
)

// It helps put the holder into the correct raft term-role pair by detecting
// the transition condition. Or more precisely, it implements the state
// machine:
//                                             == Re-elction ==
//                                            ||              ||
//                   === times out ====> {CANDIDATE} <=========
//                  ||                     ||    ||
// Starts up => {FOLLOWER} <= higher term ==      == majority vote => {LEADER}
//                  |^|                                                  ||
//                   ==================== higher term ====================
func (th *TermRoleHolder) UpgradeTerm(
	nodeId RaftNodeId,
	newTerm RaftTerm,
	reason TermUpgradeReason,
) bool {
	th.lock.Lock()
	defer th.lock.Unlock()

	if newTerm < th.term {
		return false
	}
	if newTerm == th.term &&
		reason == TUREncouteredHigherTermMessage {
		// newTerm must be a higher term than the existing.
		return false
	}

	var newRole RaftRole

	switch th.role {
	case RaftFollower:
		switch reason {
		case TURLackOfHeartbeat:
			newRole = RaftCandidate
		case TUREncouteredHigherTermMessage:
			newRole = RaftFollower
		default:
			fmt.Printf("At node=%d: illegal term upgrade reason=%s\n",
				nodeId, reason)
			return false
		}
	case RaftCandidate:
		switch reason {
		case TURLackOfHeartbeat:
			newRole = RaftCandidate
		case TURSplitVotes:
			newRole = RaftCandidate
		case TURWonAnElection:
			newRole = RaftLeader
		case TUREncouteredHigherTermMessage:
			newRole = RaftFollower
		default:
			fmt.Printf("At node=%d: illegal term upgrade reason=%s\n",
				nodeId, reason)
			return false
		}
	case RaftLeader:
		switch reason {
		case TURLackOfHeartbeat:
			newRole = RaftLeader
		case TUREncouteredHigherTermMessage:
			newRole = RaftFollower
		default:
			fmt.Printf("At node=%d: illegal term upgrade reason=%s\n",
				nodeId, reason)
			return false
		}
	}

	fmt.Printf("At node=%d: term=%d->%d role=%s->%s reason=%s\n",
		nodeId,
		th.term, newTerm,
		th.role, newRole,
		reason)

	th.term = newTerm
	th.role = newRole

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
