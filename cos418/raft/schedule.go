package raft

import (
	"context"
	"math/rand"
	"time"

	"golang.org/x/sync/semaphore"
)

const (
	HeartbeatInterval         = 100
	HeartbeatTimeoutMinMicros = 2 * HeartbeatInterval
	HeartbeatTimeoutMaxMicros = 4 * HeartbeatInterval
	ElectionTimeout           = HeartbeatTimeoutMaxMicros
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
// before the timeout, it returns true. Otherwise, it returns false.
func (fs *FollowerSchedule) WaitForHeartbeat() bool {
	clockMark := fs.clock

	timeout := time.Duration(HeartbeatTimeoutMinMicros +
		rand.Intn(HeartbeatTimeoutMaxMicros-HeartbeatTimeoutMinMicros))
	time.Sleep(timeout * time.Millisecond)

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
	preemption *semaphore.Weighted
}

func NewLeaderSchedule() *LeaderSchedule {
	ls := new(LeaderSchedule)
	ls.preemption = semaphore.NewWeighted(1000)
	return ls
}

// Wakes up the raft role maintainer thread to make it do leader work.
func (ls *LeaderSchedule) Preempt() {
	ls.preemption.Release(1)
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

	ls.preemption.Acquire(context.Background(), 1)

	canceled = true
}
