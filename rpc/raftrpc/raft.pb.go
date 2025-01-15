// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.33.0
// 	protoc        v3.6.1
// source: raft.proto

package raftrpc

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type AppendEntriesInRaftRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term         int32       `protobuf:"varint,1,opt,name=Term,proto3" json:"Term,omitempty"`
	LeaderId     int32       `protobuf:"varint,2,opt,name=LeaderId,proto3" json:"LeaderId,omitempty"`
	PrevLogIndex int32       `protobuf:"varint,3,opt,name=PrevLogIndex,proto3" json:"PrevLogIndex,omitempty"`
	PrevLogTerm  int32       `protobuf:"varint,4,opt,name=PrevLogTerm,proto3" json:"PrevLogTerm,omitempty"`
	Entries      []*LogEntry `protobuf:"bytes,5,rep,name=Entries,proto3" json:"Entries,omitempty"`
	LeaderCommit int32       `protobuf:"varint,6,opt,name=LeaderCommit,proto3" json:"LeaderCommit,omitempty"`
}

func (x *AppendEntriesInRaftRequest) Reset() {
	*x = AppendEntriesInRaftRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raft_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AppendEntriesInRaftRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AppendEntriesInRaftRequest) ProtoMessage() {}

func (x *AppendEntriesInRaftRequest) ProtoReflect() protoreflect.Message {
	mi := &file_raft_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AppendEntriesInRaftRequest.ProtoReflect.Descriptor instead.
func (*AppendEntriesInRaftRequest) Descriptor() ([]byte, []int) {
	return file_raft_proto_rawDescGZIP(), []int{0}
}

func (x *AppendEntriesInRaftRequest) GetTerm() int32 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *AppendEntriesInRaftRequest) GetLeaderId() int32 {
	if x != nil {
		return x.LeaderId
	}
	return 0
}

func (x *AppendEntriesInRaftRequest) GetPrevLogIndex() int32 {
	if x != nil {
		return x.PrevLogIndex
	}
	return 0
}

func (x *AppendEntriesInRaftRequest) GetPrevLogTerm() int32 {
	if x != nil {
		return x.PrevLogTerm
	}
	return 0
}

func (x *AppendEntriesInRaftRequest) GetEntries() []*LogEntry {
	if x != nil {
		return x.Entries
	}
	return nil
}

func (x *AppendEntriesInRaftRequest) GetLeaderCommit() int32 {
	if x != nil {
		return x.LeaderCommit
	}
	return 0
}

type LogEntry struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Command *DetailCod `protobuf:"bytes,1,opt,name=Command,proto3" json:"Command,omitempty"`
	Term    int32      `protobuf:"varint,2,opt,name=Term,proto3" json:"Term,omitempty"`
}

func (x *LogEntry) Reset() {
	*x = LogEntry{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raft_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LogEntry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LogEntry) ProtoMessage() {}

func (x *LogEntry) ProtoReflect() protoreflect.Message {
	mi := &file_raft_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LogEntry.ProtoReflect.Descriptor instead.
func (*LogEntry) Descriptor() ([]byte, []int) {
	return file_raft_proto_rawDescGZIP(), []int{1}
}

func (x *LogEntry) GetCommand() *DetailCod {
	if x != nil {
		return x.Command
	}
	return nil
}

func (x *LogEntry) GetTerm() int32 {
	if x != nil {
		return x.Term
	}
	return 0
}

type DetailCod struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Index       int32  `protobuf:"varint,1,opt,name=Index,proto3" json:"Index,omitempty"`
	Term        int32  `protobuf:"varint,2,opt,name=Term,proto3" json:"Term,omitempty"`
	OpType      string `protobuf:"bytes,3,opt,name=OpType,proto3" json:"OpType,omitempty"`
	Key         string `protobuf:"bytes,4,opt,name=Key,proto3" json:"Key,omitempty"`
	Value       string `protobuf:"bytes,5,opt,name=Value,proto3" json:"Value,omitempty"`
	SeqId       int64  `protobuf:"varint,6,opt,name=SeqId,proto3" json:"SeqId,omitempty"`
	ClientId    int64  `protobuf:"varint,7,opt,name=ClientId,proto3" json:"ClientId,omitempty"`
	FileVersion int64  `protobuf:"varint,8,opt,name=FileVersion,proto3" json:"FileVersion,omitempty"`
}

func (x *DetailCod) Reset() {
	*x = DetailCod{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raft_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DetailCod) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DetailCod) ProtoMessage() {}

func (x *DetailCod) ProtoReflect() protoreflect.Message {
	mi := &file_raft_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DetailCod.ProtoReflect.Descriptor instead.
func (*DetailCod) Descriptor() ([]byte, []int) {
	return file_raft_proto_rawDescGZIP(), []int{2}
}

func (x *DetailCod) GetIndex() int32 {
	if x != nil {
		return x.Index
	}
	return 0
}

func (x *DetailCod) GetTerm() int32 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *DetailCod) GetOpType() string {
	if x != nil {
		return x.OpType
	}
	return ""
}

func (x *DetailCod) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *DetailCod) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

func (x *DetailCod) GetSeqId() int64 {
	if x != nil {
		return x.SeqId
	}
	return 0
}

func (x *DetailCod) GetClientId() int64 {
	if x != nil {
		return x.ClientId
	}
	return 0
}

func (x *DetailCod) GetFileVersion() int64 {
	if x != nil {
		return x.FileVersion
	}
	return 0
}

type AppendEntriesInRaftResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term          int32 `protobuf:"varint,1,opt,name=Term,proto3" json:"Term,omitempty"`
	Success       bool  `protobuf:"varint,2,opt,name=Success,proto3" json:"Success,omitempty"`
	ConflictIndex int32 `protobuf:"varint,3,opt,name=ConflictIndex,proto3" json:"ConflictIndex,omitempty"`
	ConflictTerm  int32 `protobuf:"varint,4,opt,name=ConflictTerm,proto3" json:"ConflictTerm,omitempty"`
}

func (x *AppendEntriesInRaftResponse) Reset() {
	*x = AppendEntriesInRaftResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raft_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AppendEntriesInRaftResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AppendEntriesInRaftResponse) ProtoMessage() {}

func (x *AppendEntriesInRaftResponse) ProtoReflect() protoreflect.Message {
	mi := &file_raft_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AppendEntriesInRaftResponse.ProtoReflect.Descriptor instead.
func (*AppendEntriesInRaftResponse) Descriptor() ([]byte, []int) {
	return file_raft_proto_rawDescGZIP(), []int{3}
}

func (x *AppendEntriesInRaftResponse) GetTerm() int32 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *AppendEntriesInRaftResponse) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

func (x *AppendEntriesInRaftResponse) GetConflictIndex() int32 {
	if x != nil {
		return x.ConflictIndex
	}
	return 0
}

func (x *AppendEntriesInRaftResponse) GetConflictTerm() int32 {
	if x != nil {
		return x.ConflictTerm
	}
	return 0
}

type RequestVoteRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term         int32 `protobuf:"varint,1,opt,name=Term,proto3" json:"Term,omitempty"`
	CandidateId  int32 `protobuf:"varint,2,opt,name=CandidateId,proto3" json:"CandidateId,omitempty"`
	LastLogIndex int32 `protobuf:"varint,3,opt,name=LastLogIndex,proto3" json:"LastLogIndex,omitempty"`
	LastLogTerm  int32 `protobuf:"varint,4,opt,name=LastLogTerm,proto3" json:"LastLogTerm,omitempty"`
}

func (x *RequestVoteRequest) Reset() {
	*x = RequestVoteRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raft_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RequestVoteRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RequestVoteRequest) ProtoMessage() {}

func (x *RequestVoteRequest) ProtoReflect() protoreflect.Message {
	mi := &file_raft_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RequestVoteRequest.ProtoReflect.Descriptor instead.
func (*RequestVoteRequest) Descriptor() ([]byte, []int) {
	return file_raft_proto_rawDescGZIP(), []int{4}
}

func (x *RequestVoteRequest) GetTerm() int32 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *RequestVoteRequest) GetCandidateId() int32 {
	if x != nil {
		return x.CandidateId
	}
	return 0
}

func (x *RequestVoteRequest) GetLastLogIndex() int32 {
	if x != nil {
		return x.LastLogIndex
	}
	return 0
}

func (x *RequestVoteRequest) GetLastLogTerm() int32 {
	if x != nil {
		return x.LastLogTerm
	}
	return 0
}

type RequestVoteResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term        int32 `protobuf:"varint,1,opt,name=Term,proto3" json:"Term,omitempty"`
	VoteGranted bool  `protobuf:"varint,2,opt,name=VoteGranted,proto3" json:"VoteGranted,omitempty"`
}

func (x *RequestVoteResponse) Reset() {
	*x = RequestVoteResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raft_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RequestVoteResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RequestVoteResponse) ProtoMessage() {}

func (x *RequestVoteResponse) ProtoReflect() protoreflect.Message {
	mi := &file_raft_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RequestVoteResponse.ProtoReflect.Descriptor instead.
func (*RequestVoteResponse) Descriptor() ([]byte, []int) {
	return file_raft_proto_rawDescGZIP(), []int{5}
}

func (x *RequestVoteResponse) GetTerm() int32 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *RequestVoteResponse) GetVoteGranted() bool {
	if x != nil {
		return x.VoteGranted
	}
	return false
}

var File_raft_proto protoreflect.FileDescriptor

var file_raft_proto_rawDesc = []byte{
	0x0a, 0x0a, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xdb, 0x01, 0x0a,
	0x1a, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x49, 0x6e,
	0x52, 0x61, 0x66, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x54,
	0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x54, 0x65, 0x72, 0x6d, 0x12,
	0x1a, 0x0a, 0x08, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x49, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x05, 0x52, 0x08, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x49, 0x64, 0x12, 0x22, 0x0a, 0x0c, 0x50,
	0x72, 0x65, 0x76, 0x4c, 0x6f, 0x67, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x05, 0x52, 0x0c, 0x50, 0x72, 0x65, 0x76, 0x4c, 0x6f, 0x67, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x12,
	0x20, 0x0a, 0x0b, 0x50, 0x72, 0x65, 0x76, 0x4c, 0x6f, 0x67, 0x54, 0x65, 0x72, 0x6d, 0x18, 0x04,
	0x20, 0x01, 0x28, 0x05, 0x52, 0x0b, 0x50, 0x72, 0x65, 0x76, 0x4c, 0x6f, 0x67, 0x54, 0x65, 0x72,
	0x6d, 0x12, 0x23, 0x0a, 0x07, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x18, 0x05, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x09, 0x2e, 0x4c, 0x6f, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x07, 0x45,
	0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x12, 0x22, 0x0a, 0x0c, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72,
	0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x18, 0x06, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0c, 0x4c, 0x65,
	0x61, 0x64, 0x65, 0x72, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x22, 0x44, 0x0a, 0x08, 0x4c, 0x6f,
	0x67, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x24, 0x0a, 0x07, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e,
	0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0a, 0x2e, 0x44, 0x65, 0x74, 0x61, 0x69, 0x6c,
	0x43, 0x6f, 0x64, 0x52, 0x07, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x12, 0x12, 0x0a, 0x04,
	0x54, 0x65, 0x72, 0x6d, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x54, 0x65, 0x72, 0x6d,
	0x22, 0xc9, 0x01, 0x0a, 0x09, 0x44, 0x65, 0x74, 0x61, 0x69, 0x6c, 0x43, 0x6f, 0x64, 0x12, 0x14,
	0x0a, 0x05, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x05, 0x49,
	0x6e, 0x64, 0x65, 0x78, 0x12, 0x12, 0x0a, 0x04, 0x54, 0x65, 0x72, 0x6d, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x05, 0x52, 0x04, 0x54, 0x65, 0x72, 0x6d, 0x12, 0x16, 0x0a, 0x06, 0x4f, 0x70, 0x54, 0x79,
	0x70, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x4f, 0x70, 0x54, 0x79, 0x70, 0x65,
	0x12, 0x10, 0x0a, 0x03, 0x4b, 0x65, 0x79, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x4b,
	0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x05, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x53, 0x65, 0x71, 0x49,
	0x64, 0x18, 0x06, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x53, 0x65, 0x71, 0x49, 0x64, 0x12, 0x1a,
	0x0a, 0x08, 0x43, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x18, 0x07, 0x20, 0x01, 0x28, 0x03,
	0x52, 0x08, 0x43, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x20, 0x0a, 0x0b, 0x46, 0x69,
	0x6c, 0x65, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x08, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x0b, 0x46, 0x69, 0x6c, 0x65, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x22, 0x95, 0x01, 0x0a,
	0x1b, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x49, 0x6e,
	0x52, 0x61, 0x66, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x12, 0x0a, 0x04,
	0x54, 0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x54, 0x65, 0x72, 0x6d,
	0x12, 0x18, 0x0a, 0x07, 0x53, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x08, 0x52, 0x07, 0x53, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x12, 0x24, 0x0a, 0x0d, 0x43, 0x6f,
	0x6e, 0x66, 0x6c, 0x69, 0x63, 0x74, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x05, 0x52, 0x0d, 0x43, 0x6f, 0x6e, 0x66, 0x6c, 0x69, 0x63, 0x74, 0x49, 0x6e, 0x64, 0x65, 0x78,
	0x12, 0x22, 0x0a, 0x0c, 0x43, 0x6f, 0x6e, 0x66, 0x6c, 0x69, 0x63, 0x74, 0x54, 0x65, 0x72, 0x6d,
	0x18, 0x04, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0c, 0x43, 0x6f, 0x6e, 0x66, 0x6c, 0x69, 0x63, 0x74,
	0x54, 0x65, 0x72, 0x6d, 0x22, 0x90, 0x01, 0x0a, 0x12, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x56, 0x6f, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x54,
	0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x54, 0x65, 0x72, 0x6d, 0x12,
	0x20, 0x0a, 0x0b, 0x43, 0x61, 0x6e, 0x64, 0x69, 0x64, 0x61, 0x74, 0x65, 0x49, 0x64, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x05, 0x52, 0x0b, 0x43, 0x61, 0x6e, 0x64, 0x69, 0x64, 0x61, 0x74, 0x65, 0x49,
	0x64, 0x12, 0x22, 0x0a, 0x0c, 0x4c, 0x61, 0x73, 0x74, 0x4c, 0x6f, 0x67, 0x49, 0x6e, 0x64, 0x65,
	0x78, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0c, 0x4c, 0x61, 0x73, 0x74, 0x4c, 0x6f, 0x67,
	0x49, 0x6e, 0x64, 0x65, 0x78, 0x12, 0x20, 0x0a, 0x0b, 0x4c, 0x61, 0x73, 0x74, 0x4c, 0x6f, 0x67,
	0x54, 0x65, 0x72, 0x6d, 0x18, 0x04, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0b, 0x4c, 0x61, 0x73, 0x74,
	0x4c, 0x6f, 0x67, 0x54, 0x65, 0x72, 0x6d, 0x22, 0x4b, 0x0a, 0x13, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x56, 0x6f, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x12,
	0x0a, 0x04, 0x54, 0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x54, 0x65,
	0x72, 0x6d, 0x12, 0x20, 0x0a, 0x0b, 0x56, 0x6f, 0x74, 0x65, 0x47, 0x72, 0x61, 0x6e, 0x74, 0x65,
	0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0b, 0x56, 0x6f, 0x74, 0x65, 0x47, 0x72, 0x61,
	0x6e, 0x74, 0x65, 0x64, 0x32, 0xe6, 0x01, 0x0a, 0x04, 0x52, 0x61, 0x66, 0x74, 0x12, 0x52, 0x0a,
	0x13, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x49, 0x6e,
	0x52, 0x61, 0x66, 0x74, 0x12, 0x1b, 0x2e, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74,
	0x72, 0x69, 0x65, 0x73, 0x49, 0x6e, 0x52, 0x61, 0x66, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x1c, 0x2e, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65,
	0x73, 0x49, 0x6e, 0x52, 0x61, 0x66, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22,
	0x00, 0x12, 0x4e, 0x0a, 0x0f, 0x48, 0x65, 0x61, 0x72, 0x74, 0x62, 0x65, 0x61, 0x74, 0x49, 0x6e,
	0x52, 0x61, 0x66, 0x74, 0x12, 0x1b, 0x2e, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74,
	0x72, 0x69, 0x65, 0x73, 0x49, 0x6e, 0x52, 0x61, 0x66, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x1c, 0x2e, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65,
	0x73, 0x49, 0x6e, 0x52, 0x61, 0x66, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22,
	0x00, 0x12, 0x3a, 0x0a, 0x0b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74, 0x65,
	0x12, 0x13, 0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74, 0x65, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x14, 0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56,
	0x6f, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x0c, 0x5a,
	0x0a, 0x2e, 0x2f, 0x3b, 0x72, 0x61, 0x66, 0x74, 0x72, 0x70, 0x63, 0x62, 0x06, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x33,
}

var (
	file_raft_proto_rawDescOnce sync.Once
	file_raft_proto_rawDescData = file_raft_proto_rawDesc
)

func file_raft_proto_rawDescGZIP() []byte {
	file_raft_proto_rawDescOnce.Do(func() {
		file_raft_proto_rawDescData = protoimpl.X.CompressGZIP(file_raft_proto_rawDescData)
	})
	return file_raft_proto_rawDescData
}

var file_raft_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_raft_proto_goTypes = []interface{}{
	(*AppendEntriesInRaftRequest)(nil),  // 0: AppendEntriesInRaftRequest
	(*LogEntry)(nil),                    // 1: LogEntry
	(*DetailCod)(nil),                   // 2: DetailCod
	(*AppendEntriesInRaftResponse)(nil), // 3: AppendEntriesInRaftResponse
	(*RequestVoteRequest)(nil),          // 4: RequestVoteRequest
	(*RequestVoteResponse)(nil),         // 5: RequestVoteResponse
}
var file_raft_proto_depIdxs = []int32{
	1, // 0: AppendEntriesInRaftRequest.Entries:type_name -> LogEntry
	2, // 1: LogEntry.Command:type_name -> DetailCod
	0, // 2: Raft.AppendEntriesInRaft:input_type -> AppendEntriesInRaftRequest
	0, // 3: Raft.HeartbeatInRaft:input_type -> AppendEntriesInRaftRequest
	4, // 4: Raft.RequestVote:input_type -> RequestVoteRequest
	3, // 5: Raft.AppendEntriesInRaft:output_type -> AppendEntriesInRaftResponse
	3, // 6: Raft.HeartbeatInRaft:output_type -> AppendEntriesInRaftResponse
	5, // 7: Raft.RequestVote:output_type -> RequestVoteResponse
	5, // [5:8] is the sub-list for method output_type
	2, // [2:5] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_raft_proto_init() }
func file_raft_proto_init() {
	if File_raft_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_raft_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AppendEntriesInRaftRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_raft_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LogEntry); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_raft_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DetailCod); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_raft_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AppendEntriesInRaftResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_raft_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RequestVoteRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_raft_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RequestVoteResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_raft_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_raft_proto_goTypes,
		DependencyIndexes: file_raft_proto_depIdxs,
		MessageInfos:      file_raft_proto_msgTypes,
	}.Build()
	File_raft_proto = out.File
	file_raft_proto_rawDesc = nil
	file_raft_proto_goTypes = nil
	file_raft_proto_depIdxs = nil
}
