package raft

import (
	"sync"
	"time"
)

type RaftNodeId int

// Utility to help spot congested communication in an unreliable network.
type CongestionMonitor struct {
	nodeId                    RaftNodeId
	lock                      sync.Mutex
	congested                 bool
	numConsecutiveCongestions int
}

func NewCongestionMonitor(nodeId RaftNodeId) *CongestionMonitor {
	cm := new(CongestionMonitor)
	cm.nodeId = nodeId
	cm.congested = false
	cm.numConsecutiveCongestions = 0
	return cm
}

// Check if the connection to cm.nodeId is congested with previous requests.
func (cm *CongestionMonitor) Congested() bool {
	congested := cm.congested
	if !congested {
		cm.numConsecutiveCongestions = 0
		return false
	}

	cm.numConsecutiveCongestions++
	if cm.numConsecutiveCongestions > 20 {
		cm.numConsecutiveCongestions = 0
		return false
	}

	return true
}

func WakeUpCongestionMonitor(
	timeout time.Duration,
	doneCh chan bool,
) {
	time.Sleep(timeout)
	doneCh <- true
}

// This should be called before starting an asynchronous RPC. The timeout
// argument specfies how long will WaitForResult() blocks starting from this
// Begin() call.
func (cm *CongestionMonitor) Begin(
	timeout time.Duration,
) chan bool {
	cm.lock.Lock()
	cm.congested = true
	cm.lock.Unlock()

	doneCh := make(chan bool, 2)
	go WakeUpCongestionMonitor(timeout, doneCh)
	return doneCh
}

// This should be called as soon as the RPC returns.
func (cm *CongestionMonitor) Done(doneCh chan bool) {
	doneCh <- true

	cm.lock.Lock()
	cm.congested = false
	cm.lock.Unlock()
}

// Waits until the RPC returns or times out, whichever occurs first.
func (cm *CongestionMonitor) WaitForResult(doneCh chan bool) {
	<-doneCh
}
