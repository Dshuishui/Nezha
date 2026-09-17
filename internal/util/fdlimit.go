package util

import (
	"fmt"
	"syscall"
)

// RaiseFDLimit lifts this process's soft RLIMIT_NOFILE to the hard limit and reports
// what it did. It needs no privileges: raising the soft limit up to the hard limit is
// always permitted.
//
// 为什么必须做这件事，而不是让人记得先 `ulimit -n`：
//
// 每个分区常驻**一个**文件描述符（FileDescriptorPool 的 seed，见 filepool.go 里为什么
// 是一个而不是两个）。100GB 数据按 128MB 分区是 800 个分区，也就是 800 个常驻描述符，
// 再加 RocksDB 自己的 SST 描述符和 gRPC 连接，对着默认 1024 的软上限已经很紧。
//
// 而超了之后的失败方式很糟：`WriteFileAtomic` 拿不到描述符 → `persistHardState` 报错 →
// 那里是 panic（**这个 panic 本身是对的**：带着未落盘的 term/votedFor 继续投票或当 leader
// 会破坏 Raft 的安全性论证，etcd 与 CockroachDB 在 WAL 写失败时同样是 fatal）。
// 于是整个节点在某次选举中途死掉，而根因是我们自己的描述符用量。
// 2026-09-17 实测（软上限压到 128）：
//
//	panic: persist raft hard state to .../raft_state.json: ... too many open files
//	  internal/raft/persist_state.go:81 ← electionLoop
//
// 三台实验机的硬上限都是 262144，所以进程自己抬软上限就能把这一整类问题去掉，
// 不必依赖"跑前先 ulimit -n 65536"这种要人记住的步骤——忘一次就是一个中途 panic，
// 而 panic 的现场指向选举，看不出真因。
func RaiseFDLimit() string {
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		return fmt.Sprintf("读不到 RLIMIT_NOFILE（%v），沿用系统默认", err)
	}
	before := lim.Cur
	if lim.Cur >= lim.Max {
		return fmt.Sprintf("fd 软上限已等于硬上限（%d），不动", lim.Cur)
	}
	lim.Cur = lim.Max
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		return fmt.Sprintf("抬 fd 软上限 %d→%d 失败（%v），沿用 %d", before, lim.Max, err, before)
	}
	return fmt.Sprintf("fd 软上限 %d → %d（硬上限）", before, lim.Max)
}

// CheckFDHeadroom warns when the fd limit cannot cover the resident descriptors the
// partition count implies. Call it after the partition set is known.
//
// 判据按"每个分区一个常驻描述符"算，再留出 RocksDB 与 gRPC 的余量。宁可早说一句，
// 也不要等到某次选举写状态文件时 panic——那时现场指向 electionLoop，看不出真因。
func CheckFDHeadroom(partitions int) string {
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		return ""
	}
	const headroom = 256 // RocksDB 的 SST 描述符、gRPC 连接、日志文件等
	need := uint64(partitions) + headroom
	if need <= lim.Cur {
		return ""
	}
	return fmt.Sprintf("fd 可能不够：%d 个分区各常驻 1 个描述符，加 %d 余量需要 %d，"+
		"而软上限只有 %d。超了会在写 raft 状态文件时 panic（persist_state.go）。"+
		"硬上限是 %d——提高软上限，或调大 -partitionTargetMB 以减少分区数。",
		partitions, headroom, need, lim.Cur, lim.Max)
}
