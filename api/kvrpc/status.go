package kvrpc

// Values of the Err field in the KV responses. The strings are part of the wire
// protocol between the node and every client tool.
const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"       // the key does not exist
	ErrInlineValue = "ErrInlineValue" // internal: the value is inline in the store, not in the value log
	ErrWrongLeader = "ErrWrongLeader" // retry against LeaderId
	NoKey          = "NOKEY"          // Value returned with ErrNoKey
)
