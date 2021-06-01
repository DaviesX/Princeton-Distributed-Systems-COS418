package raft

import (
	"cos418/cos418/labrpc"
)

type RequestVoteArgs struct {
	CandidateTerm int
}

type RequestVoteReply struct {
	Success bool
}

// Each raft node can only vote once per term. If the raft node has a ballot
// left for the requested CandidateTerm, it votes for the client. Otherwise,
// it rejects the voting request.
func (rf *Raft) RequestVote(
	args RequestVoteArgs,
	reply *RequestVoteReply,
) {
	rf.mu.Lock()

	if args.CandidateTerm <= rf.currentTerm {
		rf.mu.Unlock()

		reply.Success = false
		return
	}

	rf.currentTerm = args.CandidateTerm

	rf.mu.Unlock()

	reply.Success = true
}

func SendRequestVote(
	target *labrpc.ClientEnd,
	args RequestVoteArgs,
	reply *RequestVoteReply,
) bool {
	ok := target.Call("Raft.RequestVote", args, reply)
	return ok
}
