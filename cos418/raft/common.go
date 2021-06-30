package raft

type RaftRole int

const (
	RaftFollower  RaftRole = 1
	RaftCandidate RaftRole = 2
	RaftLeader    RaftRole = 3
)
