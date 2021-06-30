package raft

import (
	"cos418/cos418/labrpc"
	"fmt"
	"sync"
)

const NilTerm = -1

func CollectVoteFrom(
	target *labrpc.ClientEnd,
	candidateId int,
	term int,
	logLiveness LogLiveness,
	voteCount *int,
	voteCountMutex *sync.Mutex,
) {
	var args RequestVoteArgs
	args.CandidateId = candidateId
	args.CandidateTerm = term
	args.CandidateLogProgress = logLiveness.LogProgress
	args.CandidateHighestLogTerm = logLiveness.HighestLogTerm

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
	logLiveness LogLiveness,
	peers []*labrpc.ClientEnd,
) *int {
	voteCount := new(int)
	voteCountMutex := new(sync.Mutex)

	for i := 0; i < len(peers); i++ {
		go CollectVoteFrom(
			peers[i], candidateId,
			term, logLiveness,
			voteCount, voteCountMutex)
	}

	return voteCount
}

func DoFollowerCycle(
	raft RaftInternalInterface,
) {
	raft.TermRoleHolder().ApplyPendingTermRoleUpgrade(RaftFollower)
	for raft.FollowerSchedule().WaitForHeartbeat() {
		raft.TermRoleHolder().ApplyPendingTermRoleUpgrade(RaftFollower)
	}

	// Haven't received any message from the leader for some time.
	currentTerm, _ := raft.TermRoleHolder().CurrentTermRole()
	raft.TermRoleHolder().ApplyTermRoleUpgrade(currentTerm, RaftCandidate)

	fmt.Printf("At node=%d|term=%d: no heartbeat from the leader, becoming candidate.\n",
		raft.WhoIAm(), currentTerm)
}

func DoCandidateCycle(
	raft RaftInternalInterface,
) {
	var currentTerm int

	for i := 0; i < 1; i++ {
		currentTerm, _ := raft.TermRoleHolder().CurrentTermRole()
		raft.TermRoleHolder().ApplyTermRoleUpgrade(
			currentTerm+1, RaftCandidate)

		voteCount := StartElectionAsync(
			raft.WhoIAm(),
			currentTerm+1,
			raft.LogLiveness(),
			raft.Peers())
		raft.CandidateSchedule().WaitForElectionResult()

		if *voteCount > len(raft.Peers())/2 {
			// Just won the election.
			raft.TermRoleHolder().ApplyTermRoleUpgrade(
				currentTerm+1, RaftLeader)

			fmt.Printf(
				"At node=%d|term=%d: collected enough vote=%d, becoming leader.\n",
				raft.WhoIAm(), currentTerm+1, *voteCount)
			return
		}

		// Re-election.
		fmt.Printf(
			"At node=%d|term=%d: split votes=%d, conducting re-election the %dth times.\n",
			raft.WhoIAm(), currentTerm+1, *voteCount, i+1)
	}

	raft.TermRoleHolder().ApplyTermRoleUpgrade(currentTerm+1, RaftFollower)

	fmt.Printf(
		"At node=%d|term=%d: failing to win multiple elections, becoming follower.\n",
		raft.WhoIAm(), currentTerm+1)
}

func DoLeaderCycle(
	raft RaftInternalInterface,
) {
	for !raft.TermRoleHolder().ApplyPendingTermRoleUpgrade(RaftFollower) {
		raft.PublishAndCommit()
		raft.LeaderSchedule().TakeABreak()
	}

	// Detects possible new leaders.
	currentTerm, _ := raft.TermRoleHolder().CurrentTermRole()
	fmt.Printf(
		"At node=%d|term=%d: found higher termed heartbeat, becoming follower.\n",
		raft.WhoIAm(), currentTerm)
}

// It helps put the node into the correct raft role by detecting the transition
// condition. Or more precisely, it implements the state machine:
//                                             == Re-elction ==
//                                            ||              ||
//                   === times out ====> {CANDIDATE} <=========
//                  ||                     ||    ||
// Starts up => {FOLLOWER} <= higher term ==      == majority vote => {LEADER}
//                  |^|                                                  ||
//                   ==================== higher term ====================
//
// When it transitions, it notifies the node via the updateRoleFn.
func DoRaftRole(raft RaftInternalInterface) {
	for !raft.ShouldShutdown() {
		_, role := raft.TermRoleHolder().CurrentTermRole()

		switch role {
		case RaftFollower:
			DoFollowerCycle(raft)
		case RaftCandidate:
			DoCandidateCycle(raft)
		case RaftLeader:
			DoLeaderCycle(raft)
		}
	}
}
