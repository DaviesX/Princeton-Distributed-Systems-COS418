package raft

import (
	"cos418/cos418/labrpc"
	"time"
)

func Concatenable(
	overrideFrom int,
	precedingLogTerm RaftTerm,
	localLogs []LogEntry,
	localCommitIndex int,
) bool {
	if overrideFrom > len(localLogs) {
		// Replicating too far in the future, there are missing entries.
		return false
	}

	if overrideFrom == 0 {
		// Always possible to override the entire uncommitted log history.
		return true
	}

	return precedingLogTerm == localLogs[overrideFrom-1].Term
}

type AppendEntriesArgs struct {
	LeaderId             RaftNodeId
	LeaderTerm           RaftTerm
	StartIndex           int
	PrevLogTerm          RaftTerm // For concatenability check.
	SerializedLogEntries []byte
}

type AppendEntriesReply struct {
	Concatenable bool
	TermHold     RaftTerm
}

// It handles messages sent from the client (leader). It only proceed to
// process and the message when the client sender's term is at the minimum of
// that of the current node. Then it checks if everything before the start
// index is free of conflict. This is true iff.
// log_term@k(source A) == log_term@k(source B). If so, it proceeds to
// replicate logs from the start index.
func (rf *Raft) AppendEntries(
	args AppendEntriesArgs,
	reply *AppendEntriesReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term, _ := rf.TermRoleHolder().CurrentTermRole()
	if args.LeaderTerm < term {
		// Reject an out-of-date leader.
		reply.Concatenable = false
		reply.TermHold = term
		return
	}

	rf.termRoleHolder.UpgradeTerm(
		rf.me, args.LeaderTerm, TUREncouteredHigherTermMessage)

	if !Concatenable(
		args.StartIndex,
		args.PrevLogTerm,
		rf.logs,
		rf.commitProgress) {
		// Fails to reconcile with the alien log source.
		reply.Concatenable = false
		reply.TermHold = term
		return
	}

	foreignLogs := DeserializeLogEntries(args.SerializedLogEntries)
	OverwriteWithForeignLogs(
		foreignLogs,
		&rf.logs,
		args.StartIndex,
		rf.commitProgress,
		func(updatedLogs []LogEntry) {
			rf.persist()
		})

	reply.Concatenable = true
	reply.TermHold = term
}

func SendAppendEntriesAsync(
	target *labrpc.ClientEnd,
	args AppendEntriesArgs,
	reply *AppendEntriesReply,
	ok *bool,
	ready *bool,
) {
	*ok = target.Call("Raft.AppendEntries", args, reply)
	*ready = true
}

// Unlike the above SendAppendEntriesAsync() function, this funciton is
// synchronized but will time out after 50 ms.
func SendAppendEntries(
	target *labrpc.ClientEnd,
	args AppendEntriesArgs,
	reply *AppendEntriesReply,
) bool {
	ok := false
	ready := false

	go SendAppendEntriesAsync(target, args, reply, &ok, &ready)
	for i := 0; i < 1000 && !ready; i++ {
		time.Sleep(100 * time.Microsecond)
	}

	return ok
}
