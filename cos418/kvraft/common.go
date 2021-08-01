package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

const (
	ErrNone          Err = "None"
	ErrWrongLeader   Err = "WrongLeader"
	ErrNoSuchKey     Err = "NoSuchKey"
	ErrOpOverwritten Err = "OpOverwritten"
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type OpType int

const (
	OpPut    OpType = 1
	OpAppend OpType = 2
	OpGet    OpType = 3
)

type KvNodeId = int
