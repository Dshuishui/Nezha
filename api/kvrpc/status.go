package kvrpc

// Values of the Err field in the KV responses. The strings are part of the wire
// protocol between the node and every client tool.
const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"       // the key does not exist
	ErrInlineValue = "ErrInlineValue" // internal: the value is inline in the store, not in the value log
	ErrWrongLeader = "ErrWrongLeader" // retry against LeaderId
	// ErrInternal reports that the read failed, as distinct from finding that the key is
	// absent. The two were previously indistinguishable on the wire, so an unreadable
	// partition answered "no such key" and the client believed it. Clients already treat
	// an unrecognised Err as an error (see internal/client.Get), so this needs no change
	// on their side.
	ErrInternal = "ErrInternal" // the read failed; the key's existence is unknown
	// ErrInvalidKey rejects a key the store cannot represent, rather than silently
	// rewriting it. The store reserves keys starting with a NUL byte for its own
	// metadata; everything else is stored verbatim (see raft.ValidateKey).
	ErrInvalidKey = "ErrInvalidKey"
	NoKey         = "NOKEY" // Value returned with ErrNoKey
)
