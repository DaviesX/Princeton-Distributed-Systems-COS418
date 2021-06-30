package raft

import (
	"cos418/cos418/labrpc"
	"fmt"
)

// The following properties decide how up-to-date a log source is.
type LogLiveness struct {
	LogProgress    int
	HighestLogTerm int
}

// Operations used internally by the raft role maintainer thread.
type RaftInternalInterface interface {
	// ID of the raft node.
	WhoIAm() int

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

	// Pushes out log entries to the peers and commits those that are accepted
	// by a quorum.
	PublishAndCommit()

	// Is the raft node shutting down.
	ShouldShutdown() bool
}

func (rf *Raft) WhoIAm() int {
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
	var result LogLiveness

	result.LogProgress = len(rf.logs)

	if len(rf.logs) == 0 {
		result.HighestLogTerm = 0
	} else {
		result.HighestLogTerm = rf.logs[len(rf.logs)-1].Term
	}

	return result
}

func (rf *Raft) PublishAndCommit() {
	currentTerm, _ := rf.termRoleHolder.CurrentTermRole()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	newLeaderKnowledge, err := PublishLogs(
		rf.me,
		currentTerm,
		rf.logs,
		rf.peers,
		rf.leaderKnowledge)
	if err != nil {
		fmt.Printf(
			"At node=%d|term=%d, encountered error=%s\n",
			rf.me, currentTerm, err.Error())
	}
	*rf.leaderKnowledge = *newLeaderKnowledge

	newCommitProgress := CommitProgress(*newLeaderKnowledge)

	NotifyCommitProgressAsync(
		rf.me,
		newCommitProgress,
		rf.peers,
		currentTerm)
}

func (rf *Raft) ShouldShutdown() bool {
	return rf.done
}
