package raft

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	// ErrInlineValue 表示这个 key 的 value 内联在存储引擎里，没有 valuelog 偏移。
	ErrInlineValue = "ErrInlineValue"
	ErrWrongLeader = "ErrWrongLeader"
	NoKey          = "NOKEY"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqId    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	SeqId    int64
}

type GetReply struct {
	Err   Err
	Value string
}
