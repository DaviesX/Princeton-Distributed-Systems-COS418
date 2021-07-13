package raft

import (
	"math/rand"
	"time"
)

const (
	HeartbeatInterval         = 100
	HeartbeatTimeoutMinMillis = 500
	HeartbeatTimeoutMaxMillis = 1200
	ElectionTimeout           = 400
)

// Controls the raft role maintainer thread and provides time measurement of
// heartbeat intervals.
type FollowerSchedule struct {
	clock int
}

func NewFollowerSchedule() *FollowerSchedule {
	fs := new(FollowerSchedule)
	fs.clock = 0
	return fs
}

// Tells the follower schedule that a heartbeat arrives at this particular
// moment.
func (fs *FollowerSchedule) ConfirmHeartbeat() {
	fs.clock++
}

// Waits for a heartbeat to arrive. If there is at least one heartbeat arrives
// before the timeout, it returns true. Otherwise, it returns false.
func (fs *FollowerSchedule) WaitForHeartbeat() bool {
	clockMark := fs.clock
	timeoutMillis := HeartbeatTimeoutMinMillis +
		rand.Intn(HeartbeatTimeoutMaxMillis-HeartbeatTimeoutMinMillis)
	time.Sleep(time.Duration(timeoutMillis) * time.Millisecond)
	return fs.clock > clockMark
}

// Controls the raft role maintainer thread and provides time measurement of
// election cycles.
type CandidateSchedule struct {
}

func NewCandidateSchedule() *CandidateSchedule {
	return new(CandidateSchedule)
}

// Waits until the end of an election cycle.
func (cs *CandidateSchedule) WaitForElectionResult() {
	time.Sleep(ElectionTimeout * time.Millisecond)
}

// Controls the raft role maintainer thread to balance between latency and
// efficiency.
type LeaderSchedule struct {
	preemption chan bool
}

func NewLeaderSchedule() *LeaderSchedule {
	ls := new(LeaderSchedule)
	ls.preemption = make(chan bool, 1024)
	return ls
}

// Wakes up the raft role maintainer thread to make it do leader work.
func (ls *LeaderSchedule) Preempt() {
	ls.preemption <- true
}

func WakeUpLeaderSchedule(ls *LeaderSchedule) {
	time.Sleep(HeartbeatInterval * time.Millisecond)
	ls.Preempt()
}

// Puts the raft role maintainer thread into sleep for a while, to save
// computation, unless a preemption occurs to force the thread to do leader
// work. This function isn't thread safe.
func (ls *LeaderSchedule) TakeABreak() {
	if len(ls.preemption) == 0 {
		go WakeUpLeaderSchedule(ls)
	}

	<-ls.preemption
}
