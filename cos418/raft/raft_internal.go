package raft

import (
	"cos418/cos418/labrpc"
	"fmt"
)

// The following properties decide how up-to-date a log source is.
type LogLiveness struct {
	LogProgress    int
	HighestLogTerm RaftTerm
}

// Operations used internally by the raft role maintainer thread.
type RaftInternalInterface interface {
	// ID of the raft node.
	WhoIAm() RaftNodeId

	// Peers to the raft node indexed by node ID. Note that the current raft
	// node also needs to be included in the array.
	Peers() []*labrpc.ClientEnd

	// Helps keep the raft role maintainer thread timed correctly.
	FollowerSchedule() *FollowerSchedule
	CandidateSchedule() *CandidateSchedule
	LeaderSchedule() *LeaderSchedule

	// Holding the term-role pair.
	TermRoleHolder() *TermRoleHolder

	// Extracts information to determine the log source's up-to-dateness.
	LogLiveness() LogLiveness

	// Notifies the node that it might become a leader shortly.
	PrepareForLeadership()

	// Pushes out log entries to the peers and commits those that are accepted
	// by a quorum.
	PublishAndCommit(term RaftTerm)

	// Is the raft node shutting down.
	ShouldShutdown() bool
}

func (rf *Raft) WhoIAm() RaftNodeId {
	return rf.me
}

func (rf *Raft) Peers() []*labrpc.ClientEnd {
	return rf.peers
}

func (rf *Raft) FollowerSchedule() *FollowerSchedule {
	return rf.followerSchedule
}

func (rf *Raft) CandidateSchedule() *CandidateSchedule {
	return rf.candidateSchedule
}

func (rf *Raft) LeaderSchedule() *LeaderSchedule {
	return rf.leaderSchedule
}

func (rf *Raft) TermRoleHolder() *TermRoleHolder {
	return rf.termRoleHolder
}

func (rf *Raft) LogLiveness() LogLiveness {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var result LogLiveness
	result.LogProgress = len(rf.logs)

	if len(rf.logs) == 0 {
		result.HighestLogTerm = 0
	} else {
		result.HighestLogTerm = rf.logs[len(rf.logs)-1].Term
	}

	return result
}

func (rf *Raft) PrepareForLeadership() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := 0; i < len(rf.peersLogProgress); i++ {
		rf.peersLogProgress[i] = -1
	}
}

func (rf *Raft) PublishAndCommit(term RaftTerm) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Propagates logs to the peers to replicate.
	newPeersLogProgress, err := PublishLogs(
		rf.me,
		term,
		rf.logs,
		rf.peers,
		rf.peersLogProgress)
	if err != nil {
		fmt.Printf(
			"At node=%d|term=%d, encountered error=%s\n",
			rf.me, term, err.Error())
	}
	rf.peersLogProgress = newPeersLogProgress

	// Determines which log entries can be safely committed.
	newCommitProgress := CommitProgress(newPeersLogProgress)

	// Propagates commit progress to the leader itself and to the peers.
	if newCommitProgress > rf.commitProgress {
		rf.commitProgress, rf.stateMachineProgress =
			UpdateCommitProgress(
				newCommitProgress, rf.commitProgress,
				rf.stateMachineProgress, rf.logs, rf.applyCh)

		NotifyCommitProgress(
			rf.me,
			newCommitProgress,
			newPeersLogProgress,
			rf.peers,
			term)
	}
}

func (rf *Raft) ShouldShutdown() bool {
	return rf.done
}
