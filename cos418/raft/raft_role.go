package raft

import (
	"cos418/cos418/labrpc"
	"fmt"
	"sync"
)

func CollectVoteFrom(
	target *labrpc.ClientEnd,
	candidateId RaftNodeId,
	term RaftTerm,
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
	candidateId RaftNodeId,
	term RaftTerm,
	logLiveness LogLiveness,
	peers []*labrpc.ClientEnd,
) *int {
	voteCount := new(int)
	*voteCount = 1 // Votes for itself.
	voteCountMutex := new(sync.Mutex)

	for i := 0; i < len(peers); i++ {
		if RaftNodeId(i) == candidateId {
			continue
		}

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
	if !raft.FollowerSchedule().WaitForHeartbeat() {
		currentTerm, _ := raft.TermRoleHolder().CurrentTermRole()
		raft.TermRoleHolder().UpgradeTerm(
			raft.WhoIAm(), currentTerm+1, TURLackOfHeartbeat)
	}
}

func DoCandidateCycle(
	raft RaftInternalInterface,
	termToEstablish RaftTerm,
) {
	voteCount := StartElectionAsync(
		raft.WhoIAm(),
		termToEstablish,
		raft.LogLiveness(),
		raft.Peers())
	raft.CandidateSchedule().WaitForElectionResult()

	if *voteCount > len(raft.Peers())/2 {
		// Just won the election.
		fmt.Printf("At node=%d: won an election with #votes=%d for term=%d\n",
			raft.WhoIAm(), *voteCount, termToEstablish)

		raft.PrepareForLeadership()
		raft.TermRoleHolder().UpgradeTerm(
			raft.WhoIAm(), termToEstablish, TURWonAnElection)
	} else {
		// Split votes.
		fmt.Printf("At node=%d: lost an election with #votes=%d for term=%d\n",
			raft.WhoIAm(), *voteCount, termToEstablish)

		raft.TermRoleHolder().UpgradeTerm(
			raft.WhoIAm(), termToEstablish+1, TURSplitVotes)
	}
}

func DoLeaderCycle(
	raft RaftInternalInterface,
	leaderTerm RaftTerm,
) {
	raft.PublishAndCommit(leaderTerm)
	raft.LeaderSchedule().TakeABreak()
}

// Maintains the node's current raft role.
// Follower:
// 	Detects the absence of heartbeat and transition into a candidate.
//
// Candidate:
//	Conducts election until it finds other leaders or win an election, then it
//	sets itself to a follower or leader respectively.
//
// Leader:
//	Constantly publishing and committing logs as well as sending heartbeats to
// 	the followers.
func DoRaftRole(raft RaftInternalInterface) {
	for !raft.ShouldShutdown() {
		term, role := raft.TermRoleHolder().CurrentTermRole()

		switch role {
		case RaftFollower:
			DoFollowerCycle(raft)
		case RaftCandidate:
			DoCandidateCycle(raft, term)
		case RaftLeader:
			DoLeaderCycle(raft, term)
		}
	}
}
