package raft

import (
	"runtime"
	"sync"
	"time"

	"gitee.com/dong-shuishui/FlexSync/rpc/raftrpc"
	"gitee.com/dong-shuishui/FlexSync/util"
)

// MemoryOptimizer 内存优化器
type MemoryOptimizer struct {
	raft *Raft
	
	// 配置参数
	memoryThresholdMB    int64         // 内存阈值(MB)
	minLogRetain         int           // 最少保留日志数
	checkInterval        time.Duration // 检查间隔
	
	// 运行时状态
	isOptimizing         bool          // 是否正在优化
	lastOptimizeTime     time.Time     // 上次优化时间
	optimizeCount        int           // 优化次数统计
	
	// 统计信息
	totalFreedMemoryMB   int64         // 累计释放的内存(MB)
	totalTruncatedLogs   int           // 累计截断的日志数
	
	mu sync.RWMutex
}

// NewMemoryOptimizer 创建内存优化器
func NewMemoryOptimizer(raft *Raft) *MemoryOptimizer {
	return &MemoryOptimizer{
		raft:              raft,
		memoryThresholdMB: 500,                    // 默认500MB
		minLogRetain:      10000,                  // 默认保留1万条日志
		checkInterval:     6 * time.Second,       // 默认6秒检查一次
		lastOptimizeTime:  time.Now(),
	}
}

// SetConfig 设置配置参数
func (mo *MemoryOptimizer) SetConfig(memoryThresholdMB int64, minLogRetain int, checkInterval time.Duration) {
	mo.mu.Lock()
	defer mo.mu.Unlock()
	
	mo.memoryThresholdMB = memoryThresholdMB
	mo.minLogRetain = minLogRetain
	mo.checkInterval = checkInterval
}

// Start 启动内存优化器
func (mo *MemoryOptimizer) Start() {
	util.DPrintf("MemoryOptimizer[%d] started", mo.raft.me)
	
	ticker := time.NewTicker(mo.checkInterval)
	defer ticker.Stop()
	
	for !mo.raft.killed() {
		select {
		case <-ticker.C:
			mo.checkAndOptimize()
		}
	}
	
	util.DPrintf("MemoryOptimizer[%d] stopped", mo.raft.me)
}

// checkAndOptimize 检查并优化内存
func (mo *MemoryOptimizer) checkAndOptimize() {
	mo.mu.Lock()
	if mo.isOptimizing {
		mo.mu.Unlock()
		return
	}
	mo.mu.Unlock()
	
	// 估算当前内存使用
	currentMemoryMB := mo.estimateLogMemory()
	
	if currentMemoryMB > mo.memoryThresholdMB {
		util.DPrintf("MemoryOptimizer[%d] memory usage %dMB exceeds threshold %dMB", 
			mo.raft.me, currentMemoryMB, mo.memoryThresholdMB)
		
		mo.mu.Lock()
		mo.isOptimizing = true
		mo.mu.Unlock()
		
		// 异步执行优化
		go mo.doOptimize()
	}
}

// doOptimize 执行优化
func (mo *MemoryOptimizer) doOptimize() {
	defer func() {
		mo.mu.Lock()
		mo.isOptimizing = false
		mo.lastOptimizeTime = time.Now()
		mo.optimizeCount++
		mo.mu.Unlock()
	}()
	
	mo.raft.mu.Lock()
	defer mo.raft.mu.Unlock()
	
	beforeMemoryMB := mo.estimateLogMemory()
	beforeLogCount := len(mo.raft.log)
	
	// 根据节点角色选择优化策略
	var truncateIndex int
	switch mo.raft.role {
	case ROLE_LEADER:
		truncateIndex = mo.calculateLeaderTruncateIndex()
	case ROLE_FOLLOWER:
		truncateIndex = mo.calculateFollowerTruncateIndex()
	default:
		util.DPrintf("MemoryOptimizer[%d] skip optimize for role %s", mo.raft.me, mo.raft.role)
		return
	}
	
	if truncateIndex <= mo.raft.shotOffset {
		util.DPrintf("MemoryOptimizer[%d] skip: truncateIndex(%d) <= shotOffset(%d)", 
			mo.raft.me, truncateIndex, mo.raft.shotOffset)
		return
	}
	
	// 执行截断
	mo.executeTruncate(truncateIndex)
	
	// 统计优化效果
	afterMemoryMB := mo.estimateLogMemory()
	afterLogCount := len(mo.raft.log)
	freedMemoryMB := beforeMemoryMB - afterMemoryMB
	truncatedLogs := beforeLogCount - afterLogCount
	
	mo.mu.Lock()
	mo.totalFreedMemoryMB += freedMemoryMB
	mo.totalTruncatedLogs += truncatedLogs
	mo.mu.Unlock()
	
	util.DPrintf("MemoryOptimizer[%d] optimized: freed %dMB memory, truncated %d logs, shotOffset=%d", 
		mo.raft.me, freedMemoryMB, truncatedLogs, mo.raft.shotOffset)
	
	// 建议垃圾回收
	runtime.GC()
}

// calculateLeaderTruncateIndex 计算Leader的截断索引
func (mo *MemoryOptimizer) calculateLeaderTruncateIndex() int {
	// 单节点情况
	if len(mo.raft.peers) == 1 {
		safeIndex := mo.raft.lastApplied
		if mo.getGlobalLastIndex()-safeIndex < mo.minLogRetain {
			return mo.raft.shotOffset // 不截断
		}
		return safeIndex
	}
	
	// 多节点情况：找到所有节点都已复制的最小索引
	// 首先确保Leader自己的matchIndex正确
	mo.raft.matchIndex[mo.raft.me] = mo.getGlobalLastIndex()
	
	minMatchIndex := mo.raft.matchIndex[mo.raft.me]
	for peer := range mo.raft.peers {
		if peer != mo.raft.me && mo.raft.matchIndex[peer] < minMatchIndex {
			minMatchIndex = mo.raft.matchIndex[peer]
		}
	}
	
	// 确保保留足够的日志
	if mo.getGlobalLastIndex()-minMatchIndex < mo.minLogRetain {
		return mo.raft.shotOffset // 不截断
	}
	
	return minMatchIndex
}

// calculateFollowerTruncateIndex 计算Follower的截断索引
func (mo *MemoryOptimizer) calculateFollowerTruncateIndex() int {
	safeIndex := mo.raft.lastApplied
	
	// 确保保留足够的日志
	if mo.getGlobalLastIndex()-safeIndex < mo.minLogRetain {
		return mo.raft.shotOffset // 不截断
	}
	
	return safeIndex
}

// executeTruncate 执行截断操作
func (mo *MemoryOptimizer) executeTruncate(truncateIndex int) {
	// 计算本地数组位置
	localPos := mo.globalIndexToLocalPos(truncateIndex + 1) // +1因为要从截断点后开始保留
	
	if localPos < 0 || localPos >= len(mo.raft.log) {
		util.DPrintf("MemoryOptimizer[%d] invalid localPos %d for truncateIndex %d", 
			mo.raft.me, localPos, truncateIndex)
		return
	}
	
	keepLogCount := len(mo.raft.log) - localPos
	
	// 记录截断前信息
	beforeLen := len(mo.raft.log)
	beforeCap := cap(mo.raft.log)
	
	// 创建新的日志数组，完全断开原数组引用
	newLog := make([]*raftrpc.LogEntry, keepLogCount)
	copy(newLog, mo.raft.log[localPos:])
	
	// 显式清理原数组引用，帮助GC
	for i := range mo.raft.log {
		mo.raft.log[i] = nil
	}
	mo.raft.log = newLog
	
	// 同步处理Offsets数组
	mo.truncateOffsets(truncateIndex)
	
	// 更新shotOffset
	mo.raft.shotOffset = truncateIndex
	
	util.DPrintf("MemoryOptimizer[%d] truncated: from [len:%d,cap:%d] to [len:%d,cap:%d], shotOffset=%d",
		mo.raft.me, beforeLen, beforeCap, len(mo.raft.log), cap(mo.raft.log), mo.raft.shotOffset)
}

// truncateOffsets 截断Offsets数组
func (mo *MemoryOptimizer) truncateOffsets(truncateIndex int) {
	offsetStartPos := truncateIndex - mo.raft.shotOffset
	if offsetStartPos >= 0 && offsetStartPos < len(mo.raft.Offsets) {
		keepOffsetCount := len(mo.raft.Offsets) - offsetStartPos
		newOffsets := make([]int64, keepOffsetCount)
		copy(newOffsets, mo.raft.Offsets[offsetStartPos:])
		mo.raft.Offsets = newOffsets
	}
}

// globalIndexToLocalPos 全局索引转本地位置
func (mo *MemoryOptimizer) globalIndexToLocalPos(globalIndex int) int {
	return globalIndex - mo.raft.shotOffset - 1
}

// getGlobalLastIndex 获取全局最后索引
func (mo *MemoryOptimizer) getGlobalLastIndex() int {
	return len(mo.raft.log) + mo.raft.shotOffset
}

// estimateLogMemory 估算日志内存占用(MB)
func (mo *MemoryOptimizer) estimateLogMemory() int64 {
	if len(mo.raft.log) == 0 {
		return 0
	}
	
	// 采样估算
	avgEntrySize := int64(0)
	sampleSize := 100
	if len(mo.raft.log) < sampleSize {
		sampleSize = len(mo.raft.log)
	}
	
	for i := 0; i < sampleSize; i++ {
		if mo.raft.log[i] != nil && mo.raft.log[i].Command != nil {
			entrySize := int64(100) // 结构体基础大小
			entrySize += int64(len(mo.raft.log[i].Command.Key))
			entrySize += int64(len(mo.raft.log[i].Command.Value))
			avgEntrySize += entrySize
		}
	}
	avgEntrySize = avgEntrySize / int64(sampleSize)
	
	totalMemoryBytes := avgEntrySize * int64(len(mo.raft.log))
	return totalMemoryBytes / (1024 * 1024) // 转换为MB
}

// GetStatistics 获取统计信息
func (mo *MemoryOptimizer) GetStatistics() map[string]interface{} {
	mo.mu.RLock()
	defer mo.mu.RUnlock()
	
	return map[string]interface{}{
		"optimize_count":        mo.optimizeCount,
		"total_freed_memory_mb": mo.totalFreedMemoryMB,
		"total_truncated_logs":  mo.totalTruncatedLogs,
		"last_optimize_time":    mo.lastOptimizeTime,
		"is_optimizing":         mo.isOptimizing,
		"current_memory_mb":     mo.estimateLogMemory(),
		"current_shot_offset":   mo.raft.shotOffset,
		"current_log_count":     len(mo.raft.log),
	}
}