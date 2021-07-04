package raft

import (
	"cos418/cos418/labrpc"
	"errors"
	"fmt"
	"sort"
)

// Finds a concatenation point before which rids the peer of log conflicts
// then overrides all log entries after it. To save time and bandwidth, it
// starts trying from the point where the peer was last known to replicate.
// The synchronization only fails when the peer recognizes we are outdated.
// Failing to connect to the peer doesn't count as failure, as we do expect
// network partition.
func SyncLogsWithPeer(
	publishFrom int,
	publisherTerm int,
	allLogs []LogEntry,
	peer *labrpc.ClientEnd,
	peerReplicationProgress int,
) (int, error) {
	for concatFrom := peerReplicationProgress; concatFrom >= 0; concatFrom-- {
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
// reached censensus.
func PublishLogs(
	publishFrom int,
	publisherTerm int,
	allLogs []LogEntry,
	peers []*labrpc.ClientEnd,
	peersLogProgress []int,
) ([]int, error) {
	if len(allLogs) == 0 {
		return peersLogProgress, nil
	}

	newPeersLogProgress := make([]int, len(peers))

	for i, peer := range peers {
		if i == publishFrom {
			newPeersLogProgress[i] = peersLogProgress[i]
			continue
		}

		// TODO: Makes this call async.
		newReplicationProgress, err := SyncLogsWithPeer(
			publishFrom, publisherTerm,
			allLogs,
			peer, peersLogProgress[i])
		if err != nil {
			// Leader is outdated. Let other leaders conduct synchronization
			// instead.
			return peersLogProgress, err
		}

		newPeersLogProgress[i] = newReplicationProgress
	}

	return newPeersLogProgress, nil
}

// Decides what the commit progress is based on peers' replication progress.
// The commit progress will be the smallest index that goes after all the log
// entries that get replicated by the quorum.
func CommitProgress(peersLogProgress []int) int {
	numPeers := len(peersLogProgress)

	progresses := make([]int, numPeers)
	copy(progresses, peersLogProgress)

	sort.Slice(
		progresses,
		func(i int, j int) bool {
			return progresses[i] > progresses[j]
		})
	return progresses[numPeers/2]
}

// Notifies leader's commit progress amount the peers, so peers can push
// commited logs to their state mahcine. However, it doesn't guarantee
// synchronization.
func NotifyCommitProgressAsync(
	from int,
	leaderCommitProgress int,
	targets []*labrpc.ClientEnd,
	term int,
) {
	var args NotifyCommitProgressArgs
	args.LeaderTerm = term
	args.GlobalCommitProgress = leaderCommitProgress

	for i := 0; i < len(targets); i++ {
		go SendNotifyCommitProgress(
			targets[i], args, new(NotifyCommitProgressReply))
	}
}
