// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.11.2
// source: causal.proto

package causalrpc

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
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

type AppendEntriesInCausalRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	MapLattice []byte `protobuf:"bytes,1,opt,name=map_lattice,json=mapLattice,proto3" json:"map_lattice,omitempty"`
	Version    int32  `protobuf:"varint,2,opt,name=version,proto3" json:"version,omitempty"` // version of the log.value
}

func (x *AppendEntriesInCausalRequest) Reset() {
	*x = AppendEntriesInCausalRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_causal_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AppendEntriesInCausalRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AppendEntriesInCausalRequest) ProtoMessage() {}

func (x *AppendEntriesInCausalRequest) ProtoReflect() protoreflect.Message {
	mi := &file_causal_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AppendEntriesInCausalRequest.ProtoReflect.Descriptor instead.
func (*AppendEntriesInCausalRequest) Descriptor() ([]byte, []int) {
	return file_causal_proto_rawDescGZIP(), []int{0}
}

func (x *AppendEntriesInCausalRequest) GetMapLattice() []byte {
	if x != nil {
		return x.MapLattice
	}
	return nil
}

func (x *AppendEntriesInCausalRequest) GetVersion() int32 {
	if x != nil {
		return x.Version
	}
	return 0
}

type AppendEntriesInCausalResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Success bool `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
}

func (x *AppendEntriesInCausalResponse) Reset() {
	*x = AppendEntriesInCausalResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_causal_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AppendEntriesInCausalResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AppendEntriesInCausalResponse) ProtoMessage() {}

func (x *AppendEntriesInCausalResponse) ProtoReflect() protoreflect.Message {
	mi := &file_causal_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AppendEntriesInCausalResponse.ProtoReflect.Descriptor instead.
func (*AppendEntriesInCausalResponse) Descriptor() ([]byte, []int) {
	return file_causal_proto_rawDescGZIP(), []int{1}
}

func (x *AppendEntriesInCausalResponse) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

var File_causal_proto protoreflect.FileDescriptor

var file_causal_proto_rawDesc = []byte{
	0x0a, 0x0c, 0x63, 0x61, 0x75, 0x73, 0x61, 0x6c, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x59,
	0x0a, 0x1c, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x49,
	0x6e, 0x43, 0x61, 0x75, 0x73, 0x61, 0x6c, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1f,
	0x0a, 0x0b, 0x6d, 0x61, 0x70, 0x5f, 0x6c, 0x61, 0x74, 0x74, 0x69, 0x63, 0x65, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0c, 0x52, 0x0a, 0x6d, 0x61, 0x70, 0x4c, 0x61, 0x74, 0x74, 0x69, 0x63, 0x65, 0x12,
	0x18, 0x0a, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05,
	0x52, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x22, 0x39, 0x0a, 0x1d, 0x41, 0x70, 0x70,
	0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x49, 0x6e, 0x43, 0x61, 0x75, 0x73,
	0x61, 0x6c, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x75,
	0x63, 0x63, 0x65, 0x73, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x73, 0x75, 0x63,
	0x63, 0x65, 0x73, 0x73, 0x32, 0x62, 0x0a, 0x06, 0x43, 0x41, 0x55, 0x53, 0x41, 0x4c, 0x12, 0x58,
	0x0a, 0x15, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x49,
	0x6e, 0x43, 0x61, 0x75, 0x73, 0x61, 0x6c, 0x12, 0x1d, 0x2e, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64,
	0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x49, 0x6e, 0x43, 0x61, 0x75, 0x73, 0x61, 0x6c, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1e, 0x2e, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45,
	0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x49, 0x6e, 0x43, 0x61, 0x75, 0x73, 0x61, 0x6c, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x0e, 0x5a, 0x0c, 0x2e, 0x2f, 0x3b, 0x63,
	0x61, 0x75, 0x73, 0x61, 0x6c, 0x72, 0x70, 0x63, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_causal_proto_rawDescOnce sync.Once
	file_causal_proto_rawDescData = file_causal_proto_rawDesc
)

func file_causal_proto_rawDescGZIP() []byte {
	file_causal_proto_rawDescOnce.Do(func() {
		file_causal_proto_rawDescData = protoimpl.X.CompressGZIP(file_causal_proto_rawDescData)
	})
	return file_causal_proto_rawDescData
}

var file_causal_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_causal_proto_goTypes = []interface{}{
	(*AppendEntriesInCausalRequest)(nil),  // 0: AppendEntriesInCausalRequest
	(*AppendEntriesInCausalResponse)(nil), // 1: AppendEntriesInCausalResponse
}
var file_causal_proto_depIdxs = []int32{
	0, // 0: CAUSAL.AppendEntriesInCausal:input_type -> AppendEntriesInCausalRequest
	1, // 1: CAUSAL.AppendEntriesInCausal:output_type -> AppendEntriesInCausalResponse
	1, // [1:2] is the sub-list for method output_type
	0, // [0:1] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_causal_proto_init() }
func file_causal_proto_init() {
	if File_causal_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_causal_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AppendEntriesInCausalRequest); i {
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
		file_causal_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AppendEntriesInCausalResponse); i {
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
			RawDescriptor: file_causal_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_causal_proto_goTypes,
		DependencyIndexes: file_causal_proto_depIdxs,
		MessageInfos:      file_causal_proto_msgTypes,
	}.Build()
	File_causal_proto = out.File
	file_causal_proto_rawDesc = nil
	file_causal_proto_goTypes = nil
	file_causal_proto_depIdxs = nil
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// CAUSALClient is the client API for CAUSAL service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type CAUSALClient interface {
	AppendEntriesInCausal(ctx context.Context, in *AppendEntriesInCausalRequest, opts ...grpc.CallOption) (*AppendEntriesInCausalResponse, error)
}

type cAUSALClient struct {
	cc grpc.ClientConnInterface
}

func NewCAUSALClient(cc grpc.ClientConnInterface) CAUSALClient {
	return &cAUSALClient{cc}
}

func (c *cAUSALClient) AppendEntriesInCausal(ctx context.Context, in *AppendEntriesInCausalRequest, opts ...grpc.CallOption) (*AppendEntriesInCausalResponse, error) {
	out := new(AppendEntriesInCausalResponse)
	err := c.cc.Invoke(ctx, "/CAUSAL/AppendEntriesInCausal", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CAUSALServer is the server API for CAUSAL service.
//定义了一个名为 CAUSALServer 的接口。接口中只有一个方法 AppendEntriesInCausal，
// 该方法需要传入一个上下文对象和一个指向 AppendEntriesInCausalRequest 结构体的指针，
// 返回一个指向 AppendEntriesInCausalResponse 结构体的指针。该方法还可能会返回错误。
// 可以实现 CAUSALServer 接口的具体类型。这个具体类型必须实现接口中定义的所有方法
type CAUSALServer interface {
	// AppendEntriesInCausal 方法可以向其他节点发送追加条目的请求，并且该请求要求满足因果一致性（causal consistency）的条件
	AppendEntriesInCausal(context.Context, *AppendEntriesInCausalRequest) (*AppendEntriesInCausalResponse, error)
}

// UnimplementedCAUSALServer can be embedded to have forward compatible implementations.
type UnimplementedCAUSALServer struct {
}

func (*UnimplementedCAUSALServer) AppendEntriesInCausal(context.Context, *AppendEntriesInCausalRequest) (*AppendEntriesInCausalResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AppendEntriesInCausal not implemented")
}

func RegisterCAUSALServer(s *grpc.Server, srv CAUSALServer) {
	s.RegisterService(&_CAUSAL_serviceDesc, srv)
}

func _CAUSAL_AppendEntriesInCausal_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AppendEntriesInCausalRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CAUSALServer).AppendEntriesInCausal(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/CAUSAL/AppendEntriesInCausal",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CAUSALServer).AppendEntriesInCausal(ctx, req.(*AppendEntriesInCausalRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _CAUSAL_serviceDesc = grpc.ServiceDesc{
	ServiceName: "CAUSAL",
	HandlerType: (*CAUSALServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "AppendEntriesInCausal",
			Handler:    _CAUSAL_AppendEntriesInCausal_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "causal.proto",
}
