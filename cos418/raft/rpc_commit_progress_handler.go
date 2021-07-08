package raft

import (
	"cos418/cos418/labrpc"
)

// It sends all the specified log entries in [start, end) to the targetChannel
// sequentially. It's a blocking process.
func SendLogsToStateMachine(
	logs []LogEntry,
	start int,
	end int,
	targetChannel chan ApplyMsg,
) {
	for i := start; i < end; i++ {
		var msg ApplyMsg
		msg.Index = i + 1
		msg.Command = logs[i].Command

		targetChannel <- msg
	}
}

// Synchronizes local commit progress with the commit progress requested by the
// leader and pushes all the commit log commands to the state machine connected
// to by the applyCh. It will return the updated local commit progress and
// state machine progress.
func UpdateCommitProgress(
	safeCommitProgress int,
	currentCommitProgress int,
	stateMachineProgress int,
	logs []LogEntry,
	applyCh chan ApplyMsg,
) (int, int) {
	if safeCommitProgress <= currentCommitProgress {
		// Nothing to commit.
		return currentCommitProgress, stateMachineProgress
	}

	if safeCommitProgress > len(logs) {
		panic("The safe commit progress is out of bound.")
	}

	// Update commit progress and push logs to the state machine.
	currentCommitProgress = safeCommitProgress
	if currentCommitProgress > stateMachineProgress {
		SendLogsToStateMachine(
			logs,
			stateMachineProgress, currentCommitProgress,
			applyCh)

		stateMachineProgress = currentCommitProgress
	}

	return currentCommitProgress, stateMachineProgress
}

type NotifyCommitProgressArgs struct {
	LeaderTerm         RaftTerm
	SafeCommitProgress int
}

type NotifyCommitProgressReply struct {
	Success bool
}

// Notifies the node about the global commit progress, that is, all the logs
// that have been replicated by a quorum. It will update the heartbeat clock
// as an indication that it receives a message from the leader.
func (rf *Raft) NotifyCommitProgress(
	args NotifyCommitProgressArgs,
	reply *NotifyCommitProgressReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term, _ := rf.TermRoleHolder().CurrentTermRole()
	if args.LeaderTerm < term {
		// Reject an out-of-date leader.
		reply.Success = false
		return
	}

	// The node might not participate in a past election hence not learned
	// that the term has moved up. So we will update it here (perhaps coming
	// from a heartbeat).
	rf.followerSchedule.ConfirmHeartbeat()
	rf.TermRoleHolder().UpgradeTerm(
		rf.me, args.LeaderTerm, TUREncouteredHigherTermMessage)

	rf.commitProgress, rf.stateMachineProgress =
		UpdateCommitProgress(
			args.SafeCommitProgress, rf.commitProgress,
			rf.stateMachineProgress, rf.logs, rf.applyCh)

	reply.Success = true
}

func SendNotifyCommitProgress(
	target *labrpc.ClientEnd,
	args NotifyCommitProgressArgs,
	reply *NotifyCommitProgressReply,
) bool {
	ok := target.Call("Raft.NotifyCommitProgress", args, reply)
	return ok
}
