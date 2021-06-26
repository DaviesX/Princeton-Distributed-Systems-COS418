package raft

import (
	"cos418/cos418/labrpc"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type RaftRole int

const (
	RaftFollower  RaftRole = 1
	RaftCandidate RaftRole = 2
	RaftLeader    RaftRole = 3
)

const (
	HeartbeatInterval         = 50
	HeartbeatTimeoutMinMicros = 3 * HeartbeatInterval
	HeartbeatTimeoutMaxMicros = 6 * HeartbeatInterval
	ElectionTimeout           = HeartbeatTimeoutMaxMicros
)

func CollectVoteFrom(
	target *labrpc.ClientEnd,
	candidateId int,
	term int,
	logProgress int,
	highestLogTerm int,
	voteCount *int,
	voteCountMutex *sync.Mutex,
) {
	var args RequestVoteArgs
	args.CandidateId = candidateId
	args.CandidateTerm = term
	args.CandidateLogProgress = logProgress
	args.CandidateHighestLogTerm = highestLogTerm

	var reply RequestVoteReply

	ok := SendRequestVote(target, args, &reply)
	if !ok || !reply.VoteGranted {
		return
	}

	voteCountMutex.Lock()
	*voteCount++
	voteCountMutex.Unlock()
}

func StartElectionAsync(
	candidateId int,
	term int,
	logProgress int,
	highestLogTerm int,
	peers []*labrpc.ClientEnd,
) *int {
	voteCount := new(int)
	voteCountMutex := new(sync.Mutex)

	for i := 0; i < len(peers); i++ {
		go CollectVoteFrom(
			peers[i], candidateId,
			term, logProgress, highestLogTerm,
			voteCount, voteCountMutex)
	}

	return voteCount
}

func SendHeartbeatAsync(
	from int,
	senderCommitProgress int,
	targets []*labrpc.ClientEnd,
	term int,
) {
	args := new(AppendEntriesArgs)
	args.LeaderId = from
	args.LeaderTerm = term
	args.StartIndex = 0
	args.SerializedLogEntries = make([]byte, 0)
	args.PrevLogTerm = 0
	args.GlobalCommitProgress = senderCommitProgress

	for i := 0; i < len(targets); i++ {
		if i == from {
			continue
		}

		go SendAppendEntries(targets[i], *args, new(AppendEntriesReply))
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
			logProgress := len(rf.logs)
			var highestLogTerm int
			if logProgress == 0 {
				highestLogTerm = 0
			} else {
				highestLogTerm = rf.logs[logProgress-1].Term
			}

			voteCount := StartElectionAsync(
				rf.me, termToKeep, logProgress, highestLogTerm, rf.peers)

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
			SendHeartbeatAsync(rf.me, rf.commitProgress, rf.peers, termToKeep)
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
