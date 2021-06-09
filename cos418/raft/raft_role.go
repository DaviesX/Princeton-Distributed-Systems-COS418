package raft

import (
	"cos418/cos418/labrpc"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	HeartbeatInterval         = 50
	HeartbeatTimeoutMinMicros = 3 * HeartbeatInterval
	HeartbeatTimeoutMaxMicros = 6 * HeartbeatInterval
	ElectionTimeout           = HeartbeatTimeoutMaxMicros
)

func CollectVoteFrom(
	target *labrpc.ClientEnd,
	term int,
	voteCount *int,
	voteCountMutex *sync.Mutex,
) {
	var args RequestVoteArgs
	args.CandidateTerm = term

	var reply RequestVoteReply

	ok := SendRequestVote(target, args, &reply)
	if !ok || !reply.VoteGranted {
		return
	}

	voteCountMutex.Lock()
	*voteCount++
	voteCountMutex.Unlock()
}

func StartElectionAsync(term int, peers []*labrpc.ClientEnd) *int {
	voteCount := new(int)
	voteCountMutex := new(sync.Mutex)

	for i := 0; i < len(peers); i++ {
		go CollectVoteFrom(peers[i], term, voteCount, voteCountMutex)
	}

	return voteCount
}

func SendHeartbeatAsync(
	from int,
	targets []*labrpc.ClientEnd,
	term int,
) {
	var args AppendEntriesArgs
	args.LeaderId = from
	args.LeaderTerm = term

	for i := 0; i < len(targets); i++ {
		if i == from {
			continue
		}

		go SendAppendEntries(targets[i], args, new(AppendEntriesReply))
	}
}

// It puts the node into the correct raft role by detecting the transition
// condition. Or more precisely, it implements the state machine:
//                                             == Re-elction ==
//                                            ||              ||
//                   === times out ====> {CANDIDATE} <=========
//                  ||                     ||    ||
// Starts up => {FOLLOWER} <= higher term ==      == majority vote => {LEADER}
//                  |^|                                                  ||
//                   ==================== higher term ====================
//
func MaintainRaftRole(rf *Raft) {
	const NilTerm = -1

	termToKeep := NilTerm

	for !rf.done {
		switch rf.role {
		case RaftFollower:
			lastAppendRpcTimestamp := rf.lastAppendRpcTimestamp

			timeout := time.Duration(HeartbeatTimeoutMinMicros +
				rand.Intn(HeartbeatTimeoutMaxMicros-HeartbeatTimeoutMinMicros))
			time.Sleep(timeout * time.Millisecond)

			if rf.lastAppendRpcTimestamp == lastAppendRpcTimestamp {
				// Haven't received any message from the leader for some time.
				fmt.Printf("At node=%d|term=%d: no heartbeat from the leader, becoming candidate.\n",
					rf.me, rf.currentTerm)

				rf.role = RaftCandidate
				termToKeep = rf.currentTerm + 1
			}
		case RaftCandidate:
			voteCount := StartElectionAsync(termToKeep, rf.peers)

			time.Sleep(ElectionTimeout * time.Millisecond)

			newTerm, _ := rf.GetState()

			if *voteCount > len(rf.peers)/2 {
				fmt.Printf(
					"At node=%d|term=%d: collected enough vote=%d, becoming leader.\n",
					rf.me, termToKeep, *voteCount)

				rf.role = RaftLeader
			} else if newTerm > termToKeep {
				// Found possible election cycle or even a new leader.
				fmt.Printf(
					"At node=%d|term=%d: found higher termed heartbeat, becoming follower.\n",
					rf.me, termToKeep)

				rf.role = RaftFollower
				termToKeep = NilTerm
			} else {
				// Re-election.
				fmt.Printf(
					"At node=%d|term=%d: split votes=%d, conducting re-election.\n",
					rf.me, termToKeep, *voteCount)

				termToKeep++
			}
		case RaftLeader:
			SendHeartbeatAsync(rf.me, rf.peers, termToKeep)
			time.Sleep(HeartbeatInterval * time.Millisecond)

			newTerm, _ := rf.GetState()
			if newTerm > termToKeep {
				// Found possible candidates or even a new leader.
				fmt.Printf(
					"At node=%d|term=%d: found higher termed heartbeat, becoming follower.\n",
					rf.me, rf.currentTerm)

				rf.role = RaftFollower
				termToKeep = NilTerm
			}
		}
	}
}
