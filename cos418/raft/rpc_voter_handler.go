package raft

import (
	"cos418/cos418/labrpc"
	"time"
)

type RequestVoteArgs struct {
	CandidateId             RaftNodeId
	CandidateTerm           RaftTerm
	CandidateLogProgress    int
	CandidateHighestLogTerm RaftTerm
}

type RequestVoteReply struct {
	VoteGranted bool
}

// Given the lastest log progress and highest log term other log source have,
// deduce if it covers all the committed log entries. Note that, since there is
// no way to know the global commit progress at a given moment, it checks if
// the other log source contains a super set of the commit entries.
func ContainsAllCommitedByCandidate(
	localLogs []LogEntry,
	candidateHighestLogTerm RaftTerm,
	candidateLogProgress int,
) bool {
	logProgress := len(localLogs)

	if logProgress == 0 {
		return true
	}

	highestLogTerm := localLogs[logProgress-1].Term
	if candidateHighestLogTerm > highestLogTerm {
		return true
	} else if candidateHighestLogTerm == highestLogTerm {
		return candidateLogProgress >= logProgress
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

	currentTerm, _ := rf.termRoleHolder.CurrentTermRole()
	if args.CandidateTerm <= currentTerm {
		reply.VoteGranted = false
		return
	}

	rf.termRoleHolder.UpgradeTerm(
		rf.me, args.CandidateTerm, TUREncouteredHigherTermMessage)

	if !ContainsAllCommitedByCandidate(
		rf.logs,
		args.CandidateHighestLogTerm,
		args.CandidateLogProgress) {
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
}

func SendRequestVoteAsync(
	target *labrpc.ClientEnd,
	args RequestVoteArgs,
	reply *RequestVoteReply,
	ok *bool,
	cm *CongestionMonitor,
	doneCh chan bool,
) {
	*ok = target.Call("Raft.RequestVote", args, reply)
	cm.Done(doneCh)
}

func SendRequestVote(
	target *labrpc.ClientEnd,
	args RequestVoteArgs,
	reply *RequestVoteReply,
	cm *CongestionMonitor,
) bool {
	if cm.Congested() {
		return false
	}

	ok := false
	doneCh := cm.Begin(50 * time.Millisecond)
	go SendRequestVoteAsync(target, args, reply, &ok, cm, doneCh)
	cm.WaitForResult(doneCh)

	return ok
}
