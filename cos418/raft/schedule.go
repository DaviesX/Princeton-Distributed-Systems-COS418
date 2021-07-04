package raft

import (
	"math/rand"
	"time"
)

const (
	HeartbeatInterval         = 50
	HeartbeatTimeoutMinMillis = 2 * HeartbeatInterval
	HeartbeatTimeoutMaxMillis = 4 * HeartbeatInterval
	ElectionTimeout           = HeartbeatTimeoutMaxMillis
)

// Controls the raft role maintainer thread and provides time measurement of
// heartbeat intervals.
type FollowerSchedule struct {
	clock int
}

func NewFollowerSchedule() *FollowerSchedule {
	return new(FollowerSchedule)
}

// Tells the follower schedule that a heartbeat arrives at this particular
// moment.
func (fs *FollowerSchedule) ConfirmHeartbeat() {
	fs.clock++
}

// Waits for a heartbeat to arrive. If there is at least one heartbeat arrives
// before the timeout, it returns true. Otherwise, it returns false. It also
// let the caller to perform a quick repeated task while waiting.
func (fs *FollowerSchedule) WaitForHeartbeat(preemptWithTaskFn func()) bool {
	clockMark := fs.clock

	timeoutMillis := HeartbeatTimeoutMinMillis +
		rand.Intn(HeartbeatTimeoutMaxMillis-HeartbeatTimeoutMinMillis)

	for i := 0; i < timeoutMillis; i++ {
		if preemptWithTaskFn != nil {
			preemptWithTaskFn()
		}

		time.Sleep(1 * time.Millisecond)
	}

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
	ls.preemption = make(chan bool, 256)
	return ls
}

// Wakes up the raft role maintainer thread to make it do leader work.
func (ls *LeaderSchedule) Preempt() {
	ls.preemption <- true
}

func WakeUpLeaderSchedule(ls *LeaderSchedule, canceled *bool) {
	time.Sleep(HeartbeatInterval * time.Millisecond)
	if !*canceled {
		ls.Preempt()
	}
}

// Puts the raft role maintainer thread into sleep for a while, to save
// computation, unless a preemption occurs to force the thread to do leader
// work.
func (ls *LeaderSchedule) TakeABreak() {
	canceled := false
	go WakeUpLeaderSchedule(ls, &canceled)

	<-ls.preemption

	canceled = true
}
