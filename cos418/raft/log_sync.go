package raft

type LogEntry struct {
	term    int
	command interface{}
}
