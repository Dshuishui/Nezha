// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v5.26.1
// source: kv.proto

package kvrpc

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	KV_GetInRaft_FullMethodName = "/KV/GetInRaft"
	KV_PutInRaft_FullMethodName = "/KV/PutInRaft"
)

// KVClient is the client API for KV service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type KVClient interface {
	GetInRaft(ctx context.Context, in *GetInRaftRequest, opts ...grpc.CallOption) (*GetInRaftResponse, error)
	PutInRaft(ctx context.Context, in *PutInRaftRequest, opts ...grpc.CallOption) (*PutInRaftResponse, error)
}

type kVClient struct {
	cc grpc.ClientConnInterface
}

func NewKVClient(cc grpc.ClientConnInterface) KVClient {
	return &kVClient{cc}
}

func (c *kVClient) GetInRaft(ctx context.Context, in *GetInRaftRequest, opts ...grpc.CallOption) (*GetInRaftResponse, error) {
	out := new(GetInRaftResponse)
	err := c.cc.Invoke(ctx, KV_GetInRaft_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kVClient) PutInRaft(ctx context.Context, in *PutInRaftRequest, opts ...grpc.CallOption) (*PutInRaftResponse, error) {
	out := new(PutInRaftResponse)
	err := c.cc.Invoke(ctx, KV_PutInRaft_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// KVServer is the server API for KV service.
// All implementations must embed UnimplementedKVServer
// for forward compatibility
type KVServer interface {
	GetInRaft(context.Context, *GetInRaftRequest) (*GetInRaftResponse, error)
	PutInRaft(context.Context, *PutInRaftRequest) (*PutInRaftResponse, error)
	mustEmbedUnimplementedKVServer()
}

// UnimplementedKVServer must be embedded to have forward compatible implementations.
type UnimplementedKVServer struct {
}

func (UnimplementedKVServer) GetInRaft(context.Context, *GetInRaftRequest) (*GetInRaftResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetInRaft not implemented")
}
func (UnimplementedKVServer) PutInRaft(context.Context, *PutInRaftRequest) (*PutInRaftResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PutInRaft not implemented")
}
func (UnimplementedKVServer) mustEmbedUnimplementedKVServer() {}

// UnsafeKVServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to KVServer will
// result in compilation errors.
type UnsafeKVServer interface {
	mustEmbedUnimplementedKVServer()
}

func RegisterKVServer(s grpc.ServiceRegistrar, srv KVServer) {
	s.RegisterService(&KV_ServiceDesc, srv)
}

func _KV_GetInRaft_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetInRaftRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).GetInRaft(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: KV_GetInRaft_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).GetInRaft(ctx, req.(*GetInRaftRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_PutInRaft_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PutInRaftRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).PutInRaft(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: KV_PutInRaft_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).PutInRaft(ctx, req.(*PutInRaftRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// KV_ServiceDesc is the grpc.ServiceDesc for KV service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var KV_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "KV",
	HandlerType: (*KVServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetInRaft",
			Handler:    _KV_GetInRaft_Handler,
		},
		{
			MethodName: "PutInRaft",
			Handler:    _KV_PutInRaft_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "kv.proto",
}
