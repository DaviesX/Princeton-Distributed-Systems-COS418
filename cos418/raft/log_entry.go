package raft

import (
	"bytes"
	"encoding/gob"
)

// A log entry is a command timestamped by the leader's term when it's created.
type LogEntry struct {
	Term    int
	Command interface{}
}

// Serializes a sequence of log entries into a transmittable byte array.
func SerializeLogEntries(logs []LogEntry) []byte {
	outputBuffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(outputBuffer)

	encoder.Encode(logs)

	return outputBuffer.Bytes()
}

// Deserializes content serialized by the SerializeLogEntries().
func DeserializeLogEntries(serializedLogEntries []byte) []LogEntry {
	inputBuffer := bytes.NewBuffer(serializedLogEntries)
	decoder := gob.NewDecoder(inputBuffer)

	logs := make([]LogEntry, 0)
	decoder.Decode(&logs)

	return logs
}

// Overwrites and appends the local logs with foreign logs starting from
// startIndex. Note that startIndex can set at the end of the local logs but no
// more further. After the local log content is modified, it optionally calls
// the persistFn to persist the updated local logs.
func OverwriteWithForeignLogs(
	foreignLogEntries []LogEntry,
	localLogEntries *[]LogEntry,
	startIndex int,
	commitIndex int,
	persistFn func(logs []LogEntry),
) {
	if startIndex > len(*localLogEntries) {
		panic("startIndex is after the end of the local logs.")
	}

	for i := 0; i < len(foreignLogEntries); i++ {
		targetIndex := startIndex + i

		if targetIndex < commitIndex &&
			(*localLogEntries)[targetIndex].Term != foreignLogEntries[i].Term {
			panic("Attempting to override committed logs!")
		}

		if targetIndex >= len(*localLogEntries) {
			*localLogEntries = append(*localLogEntries, foreignLogEntries[i])
			continue
		}

		if (*localLogEntries)[targetIndex].Term != foreignLogEntries[i].Term {
			// Trim everything beyond targetIndex.
			*localLogEntries = (*localLogEntries)[:targetIndex+1]
		}

		(*localLogEntries)[targetIndex] = foreignLogEntries[i]
	}

	if persistFn != nil {
		persistFn(*localLogEntries)
	}
}

// Appends a new log to the local log source and optionally calls the persistFn
// to persist the updated logs.
func AppendLog(
	command interface{},
	currentTerm int,
	logs *[]LogEntry,
	persistFn func(logs []LogEntry),
) {
	var newEntry LogEntry
	newEntry.Command = command
	newEntry.Term = currentTerm

	*logs = append(*logs, newEntry)

	if persistFn != nil {
		persistFn(*logs)
	}
}
