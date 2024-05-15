package raft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
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
	SeqId int64
}

type PutAppendReply struct {
	Err Err
	leaderId int		// 如果请求发送的不是leader，咋需要返回leader的id
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	SeqId int64
}

type GetReply struct {
	Err   Err
	Value string
	leaderId int		// 如果请求发送的不是leader，咋需要返回leader的id
}
