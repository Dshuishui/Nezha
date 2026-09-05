package raftrpc

// Regenerate the gRPC bindings after editing raft.proto. Requires protoc plus protoc-gen-go
// (version matching google.golang.org/protobuf in go.mod) and protoc-gen-go-grpc on PATH.
//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative raft.proto
