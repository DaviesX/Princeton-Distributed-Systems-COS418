package raft

import (
	"cos418/cos418/labrpc"
	"fmt"
	"sort"
	"sync"
)

// Finds a concatenation point before which rids the peer of log conflicts
// then overrides all log entries after it. To save time and bandwidth, it
// starts trying from the point where the peer was last known to replicate.
// The synchronization only fails when the peer recognizes we are outdated.
// Failing to connect to the peer doesn't count as failure, as we do expect
// network partition.
func SyncLogsWithPeer(
	publishFrom RaftNodeId,
	publisherTerm RaftTerm,
	allLogs []LogEntry,
	peer *labrpc.ClientEnd,
	peerReplicationProgress int,
	newPeerReplicationProgress *int,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	if peerReplicationProgress == len(allLogs) {
		// The peer is up-to-date.
		*newPeerReplicationProgress = peerReplicationProgress
		return
	}

	var concatFrom int
	if peerReplicationProgress == -1 {
		concatFrom = len(allLogs)
	} else {
		concatFrom = peerReplicationProgress
	}

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

		var reply AppendEntriesReply
		ok := SendAppendEntries(peer, arg, &reply)
		if !ok {
			// Peer unreachable.
			*newPeerReplicationProgress = peerReplicationProgress
			return
		}

		if reply.TermHold > publisherTerm {
			// Leader is outdated.
			fmt.Printf(
				"leader outdated: publisherTerm=%d, replicationNodeTerm=%d",
				publisherTerm, reply.TermHold)

			*newPeerReplicationProgress = peerReplicationProgress
			return
		}

		if reply.Concatenable {
			*newPeerReplicationProgress = len(allLogs)
			return
		}
	}

	panic("logical error")
}

// Broadcasts log entries around the peers, so they can reconcile conflicts
// then replicate the content. It takes in what the leader thinks each peer's
// replication progress is and tries to push it towards the end. After that,
// it returns the updated progresses for the caller to decide which logs have
// reached censensus.
func PublishLogs(
	publishFrom RaftNodeId,
	publisherTerm RaftTerm,
	allLogs []LogEntry,
	peers []*labrpc.ClientEnd,
	peersLogProgress []int,
) []int {
	if len(allLogs) == 0 {
		return peersLogProgress
	}

	var wg sync.WaitGroup
	newPeersLogProgress := make([]int, len(peers))

	for i, peer := range peers {
		if RaftNodeId(i) == publishFrom {
			newPeersLogProgress[i] = peersLogProgress[i]
			continue
		}

		wg.Add(1)
		go SyncLogsWithPeer(
			publishFrom, publisherTerm,
			allLogs,
			peer, peersLogProgress[i],
			&newPeersLogProgress[i],
			&wg)
	}

	wg.Wait()

	return newPeersLogProgress
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

// Notifies the commit progress that is safe for the peer at the moment, so
// peers can push commited logs to their state mahcine. However, it doesn't
// guarantee synchronization.
func NotifyCommitProgress(
	from RaftNodeId,
	leaderCommitProgress int,
	targetsReplicationProgress []int,
	targets []*labrpc.ClientEnd,
	term RaftTerm,
) {
	var wg sync.WaitGroup

	for i := 0; i < len(targets); i++ {
		if RaftNodeId(i) == from {
			continue
		}

		var args NotifyCommitProgressArgs
		args.LeaderTerm = term
		if targetsReplicationProgress[i] < leaderCommitProgress {
			args.SafeCommitProgress = targetsReplicationProgress[i]
		} else {
			args.SafeCommitProgress = leaderCommitProgress
		}

		wg.Add(1)
		go SendNotifyCommitProgress(
			targets[i],
			args,
			new(NotifyCommitProgressReply),
			&wg)
	}

	wg.Wait()
}
