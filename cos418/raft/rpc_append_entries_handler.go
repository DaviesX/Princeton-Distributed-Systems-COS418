package raft

import (
	"cos418/cos418/labrpc"
)

type AppendEntriesArgs struct {
	LeaderTerm int
	LeaderId   int
}

type AppendEntriesReply struct {
}

// It handles messages sent from the client (leader). It only accepts and
// applies the message when the client's term is at the minimum of that of the
// current node.
func (rf *Raft) AppendEntries(
	args AppendEntriesArgs,
	reply *AppendEntriesReply,
) {
	rf.mu.Lock()

	if args.LeaderTerm < rf.currentTerm {
		// Reject an out-of-date leader.
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.LeaderTerm
	rf.lastAppendRpcTimestamp++

	rf.mu.Unlock()
}

func SendAppendEntries(
	target *labrpc.ClientEnd,
	args AppendEntriesArgs,
	reply *AppendEntriesReply,
) bool {
	ok := target.Call("Raft.AppendEntries", args, reply)
	return ok
}
