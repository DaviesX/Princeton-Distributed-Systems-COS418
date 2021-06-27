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
