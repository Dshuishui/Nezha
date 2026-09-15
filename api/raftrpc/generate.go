package raftrpc

// Regenerate the bindings after editing raft.proto.
//
// Pin the tools to the versions in the generated files' headers, not to whatever the
// distribution packages: protoc v29.3 (Ubuntu 24.04 ships 3.21.12) and
// protoc-gen-go v1.36.10, which is the google.golang.org/protobuf version in go.mod.
// Off-by-a-patch tools rewrite the whole file with version churn, which buries the
// real change. With the right ones, regenerating an unedited raft.proto is byte-identical
// to what is committed -- worth checking first, so that any diff you then see is yours.
//
//	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10
//
// Only the message types are regenerated below. raft_grpc.pb.go changes only when the
// service definition does (a new rpc, or a changed signature), so adding a field or a
// message leaves it alone -- and leaving it alone avoids dragging in the unrelated churn
// of a newer protoc-gen-go-grpc. Add --go-grpc_out=. --go-grpc_opt=paths=source_relative
// when the service itself changes, with protoc-gen-go-grpc pinned to the version in that
// file's header.
//go:generate protoc --go_out=. --go_opt=paths=source_relative raft.proto
