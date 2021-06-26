package raft

import (
	"cos418/cos418/labrpc"
)

type RequestVoteArgs struct {
	CandidateId             int
	CandidateTerm           int
	CandidateLogProgress    int
	CandidateHighestLogTerm int
}

type RequestVoteReply struct {
	VoteGranted bool
}

// Given the lastest log progress and highest log term other log source have,
// deduce if it covers all the committed log entries.
func ContainsAllCommitedByCandidate(
	logs []LogEntry,
	commitProgress int,
	candidateHighestLogTerm int,
	candidateLogProgress int,
) bool {
	if len(logs) == 0 {
		return true
	}

	highestCommitedTerm := logs[commitProgress-1].Term
	if candidateHighestLogTerm > highestCommitedTerm {
		return true
	} else if candidateHighestLogTerm == highestCommitedTerm {
		return candidateLogProgress >= commitProgress
	} else {
		return false
	}
}

// Each raft node can only vote once per term. If the raft node has a ballot
// left for the requested CandidateTerm, it votes for the client. Otherwise,
// it rejects the voting request.
func (rf *Raft) RequestVote(
	args RequestVoteArgs,
	reply *RequestVoteReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.CandidateTerm <= rf.currentTerm ||
		!ContainsAllCommitedByCandidate(
			rf.logs, rf.commitProgress,
			args.CandidateHighestLogTerm, args.CandidateLogProgress) {
		reply.VoteGranted = false
		return
	}

	rf.currentTerm = args.CandidateTerm

	reply.VoteGranted = true
}

func SendRequestVote(
	target *labrpc.ClientEnd,
	args RequestVoteArgs,
	reply *RequestVoteReply,
) bool {
	ok := target.Call("Raft.RequestVote", args, reply)
	return ok
}
