package raft

import (
	"bytes"
	"cos418/cos418/labrpc"
	"encoding/gob"
	"errors"
	"fmt"
	"sort"
)

// A log entry is a command timestamped by the leader's term when it's created.
type LogEntry struct {
	Term    int
	Command interface{}
}

// Serializes a sequence of log entries into a transmittable byte array.
func SerializeLogEntries(logs []LogEntry) []byte {
	outputBuffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(outputBuffer)

	encoder.Encode(logs)

	return outputBuffer.Bytes()
}

// Deserializes content serialized by the SerializeLogEntries().
func DeserializeLogEntries(serializedLogEntries []byte) []LogEntry {
	inputBuffer := bytes.NewBuffer(serializedLogEntries)
	decoder := gob.NewDecoder(inputBuffer)

	logs := make([]LogEntry, 0)
	decoder.Decode(&logs)

	return logs
}

// Finds a concatenation point before which rids the peer of log conflicts
// then overrides all log entries after it. To save time and bandwidth, it
// starts trying from the point where the peer was last known to replicate.
// The synchronization only fails when the peer recognizes we are outdated.
// Failing to connect to the peer doesn't count as failure, as we do expect
// network partition.
func SyncLogsWithPeer(
	publishFrom int,
	publisherTerm int,
	publisherCommitProgress int,
	allLogs []LogEntry,
	peer *labrpc.ClientEnd,
	peerReplicationProgress int,
) (int, error) {
	concatFrom := peerReplicationProgress
	for ; concatFrom >= 0; concatFrom-- {
		var arg AppendEntriesArgs
		arg.LeaderId = publishFrom
		arg.LeaderTerm = publisherTerm
		if concatFrom >= len(allLogs) {
			arg.StartIndex = len(allLogs)
			arg.SerializedLogEntries = SerializeLogEntries(
				make([]LogEntry, 0))
		} else {
			arg.StartIndex = concatFrom
			arg.SerializedLogEntries = SerializeLogEntries(
				allLogs[concatFrom:])
		}
		if concatFrom == 0 {
			arg.PrevLogTerm = 0
		} else {
			arg.PrevLogTerm = allLogs[concatFrom-1].Term
		}
		arg.GlobalCommitProgress = publisherCommitProgress

		var reply AppendEntriesReply
		ok := SendAppendEntries(peer, arg, &reply)
		if !ok {
			// Peer unreachable.
			return peerReplicationProgress, nil
		}

		if reply.TermHold > publisherTerm {
			errorMsg := fmt.Sprintf(
				"leader outdated: publisherTerm=%d, replicationNodeTerm=%d",
				publisherTerm, reply.TermHold)
			return 0, errors.New(errorMsg)
		}

		if reply.Concatenable {
			return len(allLogs), nil
		}
	}

	return 0, errors.New("logical error")
}

// Broadcasts log entries around the peers, so they can reconcile conflicts
// then replicate the content. It takes in what the leader thinks each peer's
// replication progress is and tries to push it towards the end. After that,
// it returns the updated progresses for the caller to decide which logs have
// reached censensus. Note, it will skip publishing to the publisher's own
// node.
func PublishLogs(
	publishFrom int,
	publisherTerm int,
	publisherCommitProgress int,
	allLogs []LogEntry,
	peers []*labrpc.ClientEnd,
	leaderKnowledge *LeaderKnowledge,
) (*LeaderKnowledge, error) {
	if len(allLogs) == 0 {
		return leaderKnowledge, nil
	}

	newLeaderKnowledge := NewLeaderKnowledge(len(peers))

	for i, peer := range peers {
		if i == publishFrom {
			newLeaderKnowledge.peerLogProgresses[i] =
				leaderKnowledge.peerLogProgresses[i]
			continue
		}

		// TODO: Makes this call async.
		newReplicationProgress, err := SyncLogsWithPeer(
			publishFrom, publisherTerm, publisherCommitProgress,
			allLogs,
			peer, leaderKnowledge.peerLogProgresses[i])
		if err != nil {
			// Leader is outdated. Let other leaders conduct synchronization
			// instead.
			return leaderKnowledge, err
		}

		newLeaderKnowledge.peerLogProgresses[i] =
			newReplicationProgress
	}

	return newLeaderKnowledge, nil
}

// Decides what the commit progress is based on peers' replication progress.
// The commit progress will be the smallest index that goes after all the log
// entries that get replicated by the quorum.
func CommitProgress(leaderKnowledge LeaderKnowledge) int {
	numPeers := len(leaderKnowledge.peerLogProgresses)

	progresses := make([]int, numPeers)
	copy(progresses, leaderKnowledge.peerLogProgresses)
	sort.Slice(
		progresses,
		func(i int, j int) bool {
			return progresses[i] > progresses[j]
		})
	if len(leaderKnowledge.peerLogProgresses)%2 == 0 {
		return progresses[numPeers/2]
	} else {
		return progresses[numPeers/2+1]
	}
}

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
		msg.Index = i
		msg.Command = logs[i].Command

		targetChannel <- msg
	}
}

// Synchronizes local commit progress with the global commit progress and
// pushes all the commit log commands to the state machine connected to by the
// applyCh. It will return the updated local commit progress and state machine
// progress.
func SyncWithGlobalCommitProgress(
	globalCommitProgress int,
	currentCommitProgress int,
	stateMachineProgress int,
	logs []LogEntry,
	applyCh chan ApplyMsg,
) (int, int) {
	if globalCommitProgress <= currentCommitProgress {
		// Nothing to commit.
		return currentCommitProgress, stateMachineProgress
	}

	// Update commit progress and push logs to the state machine.
	if globalCommitProgress <= len(logs) {
		currentCommitProgress = globalCommitProgress
	} else {
		currentCommitProgress = len(logs)
	}

	if currentCommitProgress > stateMachineProgress {
		go SendLogsToStateMachine(
			logs,
			stateMachineProgress, currentCommitProgress,
			applyCh)

		stateMachineProgress = currentCommitProgress
	}

	return currentCommitProgress, stateMachineProgress
}
