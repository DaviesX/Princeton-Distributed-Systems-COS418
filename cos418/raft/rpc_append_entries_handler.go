package raft

import (
	"cos418/cos418/labrpc"
)

func ReplicateForeignLogs(
	foreignLogData []byte,
	startIndex int,
	localLogEntries []LogEntry,
) []LogEntry {
	foreignLogEntries := DeserializeLogEntries(foreignLogData)

	for i := 0; i < len(foreignLogEntries); i++ {
		targetIndex := startIndex + i
		if targetIndex >= len(localLogEntries) {
			localLogEntries = append(localLogEntries, foreignLogEntries[i])
		} else {
			localLogEntries[targetIndex] = foreignLogEntries[i]
		}
	}

	return localLogEntries
}

type AppendEntriesArgs struct {
	LeaderTerm           int
	LeaderId             int
	StartIndex           int
	PrevLogTerm          int // For concatenability check.
	SerializedLogEntries []byte
}

type AppendEntriesReply struct {
	Concatenable bool
	TermHold     int
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

	if args.LeaderTerm < rf.currentTerm {
		// Reject an out-of-date leader.
		reply.Concatenable = false
		reply.TermHold = rf.currentTerm
		return
	}

	if len(rf.logs) > 0 && args.StartIndex > 0 {
		if args.StartIndex > len(rf.logs) ||
			rf.logs[args.StartIndex-1].Term != args.PrevLogTerm {
			// Fails to reconcile with the alien log source.
			reply.Concatenable = false
			reply.TermHold = rf.currentTerm
			return
		}
	}

	rf.logs = ReplicateForeignLogs(
		args.SerializedLogEntries, args.StartIndex, rf.logs)
	rf.persist()

	reply.Concatenable = true
	reply.TermHold = rf.currentTerm
}

func SendAppendEntries(
	target *labrpc.ClientEnd,
	args AppendEntriesArgs,
	reply *AppendEntriesReply,
) bool {
	ok := target.Call("Raft.AppendEntries", args, reply)
	return ok
}
