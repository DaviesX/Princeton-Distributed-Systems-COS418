package raft

import "cos418/cos418/labrpc"

type AppendEntriesArgs struct {
	leaderTerm int
	leaderId   int
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

	if args.leaderTerm < rf.currentTerm {
		// Reject an out-of-date leader.
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.leaderTerm
	rf.lastApplied++

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
