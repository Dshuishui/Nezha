package pool

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// freePort reserves a loopback port and releases it so the test can dial it before
// anything listens there.
func freePort(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// TestDialBeforePeerListens pins the three properties the Raft layer depends on:
// Dial succeeds while the peer is down (nodes may start in any order), an RPC against
// the dead peer fails fast with Unavailable instead of waiting for its deadline (the
// election tally counts such failures), and the same channel connects once the peer
// comes up.
func TestDialBeforePeerListens(t *testing.T) {
	addr := freePort(t)

	cc, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial with no listener: %v", err)
	}
	defer cc.Close()
	client := grpc_health_v1.NewHealthClient(cc)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err = client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	cancel()
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("RPC to dead peer: got %v, want codes.Unavailable", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("RPC to dead peer took %v; expected fail-fast", elapsed)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	// The channel is backing off after the refused connection (BackoffMaxDelay caps the
	// wait), so retry until it reconnects.
	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("channel never recovered after the peer started: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
