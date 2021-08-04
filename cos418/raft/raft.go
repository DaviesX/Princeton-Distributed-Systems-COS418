package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"cos418/cos418/labrpc"
	"encoding/gob"
	"fmt"
	"sync"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Term        RaftTerm
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu                     sync.Mutex
	peers                  []*labrpc.ClientEnd
	peersCongestionMonitor []*CongestionMonitor
	persister              *Persister
	me                     RaftNodeId // index into peers[]
	applyCh                chan ApplyMsg

	// Raft role schedules.
	followerSchedule  *FollowerSchedule
	candidateSchedule *CandidateSchedule
	leaderSchedule    *LeaderSchedule

	// Persistent state.
	termRoleHolder *TermRoleHolder
	logs           []LogEntry

	// Volatile state.
	commitProgress       int
	stateMachineProgress int
	peersLogProgress     []int

	done bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	term, role := rf.termRoleHolder.CurrentTermRole()
	return int(term), role == RaftLeader
}

// Saves Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	outputBuffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(outputBuffer)

	rf.termRoleHolder.Serialize(encoder)
	encoder.Encode(rf.logs)

	data := outputBuffer.Bytes()
	rf.persister.SaveRaftState(data)
}

// Restores previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	inputBuffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(inputBuffer)

	rf.termRoleHolder.Deserialize(decoder)
	decoder.Decode(&rf.logs)
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear as
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(
	command interface{},
) (int, int, bool) {
	currentTerm, role := rf.TermRoleHolder().CurrentTermRole()

	switch role {
	case RaftLeader:
		rf.mu.Lock()

		AppendLog(
			command, currentTerm,
			&rf.logs,
			func(updatedLogs []LogEntry) {
				rf.persist()
			})

		rf.mu.Unlock()

		fmt.Printf("At node=%d: ** added log=(command=%v, index=%d, term=%d)\n",
			rf.me, command, len(rf.logs), rf.logs[len(rf.logs)-1].Term)

		// The raft role maintainer thread will handle the replication
		// automatically.

		return len(rf.logs), int(currentTerm), true
	default:
		return -1, int(currentTerm), false
	}
}

// The tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	rf.done = true
}

// The service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := new(Raft)
	rf.peers = peers
	rf.peersCongestionMonitor = make([]*CongestionMonitor, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.peersCongestionMonitor[i] = NewCongestionMonitor(
			RaftNodeId(i))
	}
	rf.persister = persister
	rf.me = RaftNodeId(me)
	rf.applyCh = applyCh

	rf.followerSchedule = NewFollowerSchedule()
	rf.candidateSchedule = NewCandidateSchedule()
	rf.leaderSchedule = NewLeaderSchedule()

	rf.termRoleHolder = NewTermRoleHolder()
	rf.logs = make([]LogEntry, 0)

	rf.commitProgress = 0
	rf.stateMachineProgress = 0
	rf.peersLogProgress = make([]int, len(peers))

	rf.done = false

	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go DoRaftRole(rf)

	return rf
}
