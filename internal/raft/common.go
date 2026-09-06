package raft

import "gitee.com/dong-shuishui/FlexSync/api/kvrpc"

// Result strings shared with the KV service; the definitions live with the API.
const (
	OK             = kvrpc.OK
	ErrNoKey       = kvrpc.ErrNoKey
	ErrInlineValue = kvrpc.ErrInlineValue
	ErrWrongLeader = kvrpc.ErrWrongLeader
	NoKey          = kvrpc.NoKey
)
