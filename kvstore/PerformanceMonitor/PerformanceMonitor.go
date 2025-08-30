package performancemonitor

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// PerformanceMetrics 性能指标结构体
type PerformanceMetrics struct {
	Timestamp    time.Time
	// 内存指标
	MemAllocMB   float64 // 当前分配内存 (MB)
	MemSysMB    float64 // 系统内存使用 (MB)
	MemHeapMB   float64 // 堆内存使用 (MB)
	GCPauseUS   float64 // GC暂停时间 (微秒)
	NumGC       uint32  // GC次数
	
	// 磁盘IO指标  
	DiskReadsKB  float64 // 磁盘读取 (KB)
	DiskWritesKB float64 // 磁盘写入 (KB)
	DiskReadOps  float64 // 磁盘读操作次数
	DiskWriteOps float64 // 磁盘写操作次数
	IOWaitPct    float64 // IO等待百分比
	
	// CPU指标
	NumGoroutine int     // goroutine数量
	CPUUsagePct  float64 // CPU使用率估算
	
	// 进程级IO指标 (从/proc/self/io读取)
	ProcReadBytes  uint64 // 进程读取字节数
	ProcWriteBytes uint64 // 进程写入字节数
}

// PerformanceMonitor 性能监控器
type PerformanceMonitor struct {
	outputFile   *os.File
	writer       *bufio.Writer
	interval     time.Duration
	stopCh       chan bool
	wg           sync.WaitGroup
	lastIOStat   map[string]uint64
	startTime    time.Time
}

// NewPerformanceMonitor 创建新的性能监控器
func NewPerformanceMonitor(filename string, intervalMS int) (*PerformanceMonitor, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	pm := &PerformanceMonitor{
		outputFile: file,
		writer:     bufio.NewWriter(file),
		interval:   time.Duration(intervalMS) * time.Millisecond,
		stopCh:     make(chan bool),
		lastIOStat: make(map[string]uint64),
		startTime:  time.Now(),
	}

	// 写入CSV头部
	pm.writeHeader()
	return pm, nil
}

// writeHeader 写入CSV文件头部
func (pm *PerformanceMonitor) writeHeader() {
	header := "timestamp,elapsed_sec,mem_alloc_mb,mem_sys_mb,mem_heap_mb," +
		"gc_pause_us,num_gc,disk_read_kb,disk_write_kb,disk_read_ops,disk_write_ops," +
		"io_wait_pct,num_goroutine,cpu_usage_pct,proc_read_bytes,proc_write_bytes\n"
	pm.writer.WriteString(header)
	pm.writer.Flush()
}

// Start 启动监控goroutine
func (pm *PerformanceMonitor) Start() {
	pm.wg.Add(1)
	go func() {
		defer pm.wg.Done()
		ticker := time.NewTicker(pm.interval)
		defer ticker.Stop()

		for {
			select {
			case <-pm.stopCh:
				return
			case <-ticker.C:
				metrics := pm.collectMetrics()
				pm.writeMetrics(metrics)
			}
		}
	}()
}

// Stop 停止监控
func (pm *PerformanceMonitor) Stop() {
	close(pm.stopCh)
	pm.wg.Wait()
	pm.writer.Flush()
	pm.outputFile.Close()
}

// collectMetrics 收集性能指标
func (pm *PerformanceMonitor) collectMetrics() PerformanceMetrics {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	metrics := PerformanceMetrics{
		Timestamp:    time.Now(),
		MemAllocMB:   float64(m.Alloc) / 1024 / 1024,
		MemSysMB:     float64(m.Sys) / 1024 / 1024,
		MemHeapMB:    float64(m.HeapAlloc) / 1024 / 1024,
		NumGC:        m.NumGC,
		NumGoroutine: runtime.NumGoroutine(),
	}

	// GC暂停时间 (最近一次)
	if m.NumGC > 0 {
		metrics.GCPauseUS = float64(m.PauseNs[(m.NumGC+255)%256]) / 1000
	}

	// 收集磁盘IO统计 (从/proc/diskstats)
	diskStats := pm.readDiskStats()
	if len(diskStats) > 0 {
		metrics.DiskReadsKB = diskStats["read_kb"]
		metrics.DiskWritesKB = diskStats["write_kb"] 
		metrics.DiskReadOps = diskStats["read_ops"]
		metrics.DiskWriteOps = diskStats["write_ops"]
	}

	// IO等待百分比 (从/proc/stat)
	metrics.IOWaitPct = pm.readIOWaitPercent()

	// CPU使用率估算 (基于goroutine活跃度的简化版本)
	metrics.CPUUsagePct = pm.estimateCPUUsage()

	// 进程IO统计 (从/proc/self/io)
	procIO := pm.readProcIOStats()
	metrics.ProcReadBytes = procIO["read_bytes"]
	metrics.ProcWriteBytes = procIO["write_bytes"]

	return metrics
}

// readDiskStats 读取磁盘IO统计
func (pm *PerformanceMonitor) readDiskStats() map[string]float64 {
	file, err := os.Open("/proc/diskstats")
	if err != nil {
		return nil
	}
	defer file.Close()

	stats := make(map[string]float64)
	var totalReadKB, totalWriteKB, totalReadOps, totalWriteOps float64

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 14 {
			continue
		}

		// 只统计物理磁盘 (跳过分区和虚拟设备)
		deviceName := fields[2]
		if strings.Contains(deviceName, "sd") || strings.Contains(deviceName, "nvme") {
			if readOps, err := strconv.ParseFloat(fields[3], 64); err == nil {
				totalReadOps += readOps
			}
			if writeOps, err := strconv.ParseFloat(fields[7], 64); err == nil {
				totalWriteOps += writeOps
			}
			// 读取扇区数转换为KB (1扇区=512字节)
			if readSectors, err := strconv.ParseFloat(fields[5], 64); err == nil {
				totalReadKB += readSectors * 512 / 1024
			}
			if writeSectors, err := strconv.ParseFloat(fields[9], 64); err == nil {
				totalWriteKB += writeSectors * 512 / 1024
			}
		}
	}

	stats["read_kb"] = totalReadKB
	stats["write_kb"] = totalWriteKB  
	stats["read_ops"] = totalReadOps
	stats["write_ops"] = totalWriteOps

	return stats
}

// readIOWaitPercent 读取IO等待百分比
func (pm *PerformanceMonitor) readIOWaitPercent() float64 {
	file, err := os.Open("/proc/stat")
	if err != nil {
		return 0
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 6 && fields[0] == "cpu" {
			if iowait, err := strconv.ParseFloat(fields[5], 64); err == nil {
				// 简化计算，实际应该计算相对于总CPU时间的百分比
				return iowait / 100.0 // 这里做简化处理
			}
		}
	}
	return 0
}

// estimateCPUUsage 估算CPU使用率
func (pm *PerformanceMonitor) estimateCPUUsage() float64 {
	// 基于goroutine数量的简化估算
	// 实际应用中可以使用更精确的方法，如读取/proc/stat计算CPU使用率
	goroutines := float64(runtime.NumGoroutine())
	cpuCores := float64(runtime.NumCPU())
	
	// 简单的启发式估算
	usage := (goroutines / cpuCores) * 10.0 // 调整系数
	if usage > 100 {
		usage = 100
	}
	return usage
}

// readProcIOStats 读取进程IO统计
func (pm *PerformanceMonitor) readProcIOStats() map[string]uint64 {
	stats := make(map[string]uint64)
	file, err := os.Open("/proc/self/io")
	if err != nil {
		return stats
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			key := strings.TrimSuffix(fields[0], ":")
			if val, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
				stats[key] = val
			}
		}
	}
	return stats
}

// writeMetrics 写入指标到文件
func (pm *PerformanceMonitor) writeMetrics(m PerformanceMetrics) {
	elapsedSec := m.Timestamp.Sub(pm.startTime).Seconds()
	
	line := fmt.Sprintf("%.3f,%.2f,%.2f,%.2f,%.2f,%.2f,%d,%.2f,%.2f,%.0f,%.0f,%.2f,%d,%.2f,%d,%d\n",
		float64(m.Timestamp.Unix()) + float64(m.Timestamp.Nanosecond())/1e9, // 精确时间戳
		elapsedSec,
		m.MemAllocMB,
		m.MemSysMB, 
		m.MemHeapMB,
		m.GCPauseUS,
		m.NumGC,
		m.DiskReadsKB,
		m.DiskWritesKB,
		m.DiskReadOps,
		m.DiskWriteOps,
		m.IOWaitPct,
		m.NumGoroutine,
		m.CPUUsagePct,
		m.ProcReadBytes,
		m.ProcWriteBytes,
	)
	
	pm.writer.WriteString(line)
	pm.writer.Flush() // 立即刷新到磁盘
}

// // 使用示例
// func main() {
// 	// 创建性能监控器，每500ms采样一次
// 	monitor, err := NewPerformanceMonitor("performance_metrics.csv", 500)
// 	if err != nil {
// 		panic(err)
// 	}

// 	// 启动监控
// 	fmt.Println("开始性能监控...")
// 	monitor.Start()

// 	// 这里放你的主程序逻辑
// 	// 模拟一些工作负载
// 	for i := 0; i < 1000; i++ {
// 		// 模拟内存分配
// 		data := make([]byte, 1024*1024) // 1MB
// 		_ = data
		
// 		// 模拟磁盘写入
// 		file, _ := os.Create(fmt.Sprintf("temp_%d.dat", i))
// 		file.Write(make([]byte, 4096)) // 4KB写入
// 		file.Close()
// 		os.Remove(fmt.Sprintf("temp_%d.dat", i))
		
// 		time.Sleep(10 * time.Millisecond)
// 	}

// 	// 停止监控
// 	fmt.Println("停止性能监控...")
// 	monitor.Stop()
// 	fmt.Println("性能数据已保存到 performance_metrics.csv")
// }