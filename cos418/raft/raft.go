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
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type RaftRole int

const (
	RaftFollower  RaftRole = 1
	RaftCandidate RaftRole = 2
	RaftLeader    RaftRole = 3
)

type LogEntry struct {
	term    int
	command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	applyCh   chan ApplyMsg

	// Persistent state.
	currentTerm int
	logs        []LogEntry

	// Volatile state.
	role                   RaftRole
	lastAppendRpcTimestamp int

	done bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.role == RaftLeader
}

// Saves Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	outputBuffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(outputBuffer)

	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.logs)

	data := outputBuffer.Bytes()
	rf.persister.SaveRaftState(data)
}

// Restores previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	inputBuffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(inputBuffer)

	decoder.Decode(&rf.currentTerm)
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	switch rf.role {
	case RaftLeader:
		return -1, rf.currentTerm, true
	default:
		return -1, rf.currentTerm, false
	}
}

// The tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	fmt.Printf("At node=%d, stopping the raft role maintainer...\n", rf.me)

	// TODO: Sychronizes with the raft role maintainer.
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.logs = make([]LogEntry, 0)

	rf.role = RaftFollower
	rf.lastAppendRpcTimestamp = 0

	rf.done = false

	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	fmt.Printf("At node=%d, starting the raft role maintainer...\n", rf.me)
	go MaintainRaftRole(rf)

	return rf
}
