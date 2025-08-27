package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	// "sync"
	"time"

	"github.com/tecbot/gorocksdb"
)

type TestType int

const (
	FullValue  TestType = iota // 完整值存储(传统方式)
	OffsetOnly                 // 仅偏移量存储(键值分离方式)
)

func (t TestType) String() string {
	switch t {
	case FullValue:
		return "传统方式"
	case OffsetOnly:
		return "键值分离"
	default:
		return "未知"
	}
}

type WALComparisonTest struct {
	DBPath      string
	TestType    TestType
	RecordCount int
	ValueSize   int
	DB          *gorocksdb.DB
	Options     *gorocksdb.Options
	LogFile     *os.File
}

type ComparisonResult struct {
	TestType      string
	RecordCount   int
	ValueSize     int
	WALSize       int64
	RecoveryTime  time.Duration
	CPUPeak       float64
	MemoryBefore  uint64
	MemoryPeak    uint64
	MemoryAfter   uint64
	DataIntegrity bool
	WALSizeAtWrite int64        // 写入完成时的WAL大小
    WALSizeAtRecovery int64     // 恢复时的WAL大小（用于调试）
}

func NewWALComparisonTest(testType TestType, recordCount int, valueSize int) *WALComparisonTest {
	dbPath := fmt.Sprintf("./test_comparison_%s_%dk_records", 
		strings.ToLower(testType.String()), recordCount/1000)
	
	return &WALComparisonTest{
		DBPath:      dbPath,
		TestType:    testType,
		RecordCount: recordCount,
		ValueSize:   valueSize,
	}
}

// 只输出到日志文件
func (wct *WALComparisonTest) logfToFile(format string, args ...interface{}) {
	if wct.LogFile != nil {
		message := fmt.Sprintf(format, args...)
		wct.LogFile.WriteString(message)
		wct.LogFile.Sync()
	}
}

// 获取当前内存使用情况
func getMemoryUsage() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc
}

// 监控内存峰值
func (wct *WALComparisonTest) monitorMemoryPeak(done chan bool) <-chan uint64 {
	memChan := make(chan uint64, 1)
	
	go func() {
		var peak uint64
		ticker := time.NewTicker(10 * time.Millisecond) // 每10ms检查一次
		defer ticker.Stop()
		
		for {
			select {
			case <-done:
				memChan <- peak
				return
			case <-ticker.C:
				current := getMemoryUsage()
				if current > peak {
					peak = current
				}
			}
		}
	}()
	
	return memChan
}

// 监控CPU使用率峰值 (简化版本，实际应用中可以使用更精确的方法)
func (wct *WALComparisonTest) monitorCPUPeak(done chan bool) <-chan float64 {
	cpuChan := make(chan float64, 1)
	
	go func() {
		var peak float64
		ticker := time.NewTicker(50 * time.Millisecond) // 每50ms检查一次
		defer ticker.Stop()
		
		for {
			select {
			case <-done:
				cpuChan <- peak
				return
			case <-ticker.C:
				// 简化的CPU使用率估算，基于内存分配变化
				// 实际应用中可以使用第三方库获取真实CPU使用率
				current := float64(runtime.NumGoroutine()) * 0.1 // 简化估算
				if current > peak {
					peak = current
				}
			}
		}
	}()
	
	return cpuChan
}

// 配置RocksDB选项
// func (wct *WALComparisonTest) setupDBOptions() {
// 	wct.Options = gorocksdb.NewDefaultOptions()
// 	wct.Options.SetCreateIfMissing(true)
	
// 	// 设置较大的写入缓冲区确保数据在WAL中
// 	wct.Options.SetWriteBufferSize(10 * 1024 * 1024 * 1024) // 10GB
// 	wct.Options.SetMaxWriteBufferNumber(3)
// 	wct.Options.SetMinWriteBufferNumberToMerge(1)
	
// 	// 禁用自动压缩
// 	wct.Options.SetDisableAutoCompactions(true)
// 	wct.Options.SetLevel0FileNumCompactionTrigger(1000)
	
// 	wct.logfToFile("配置数据库选项 - 测试类型: %s, 记录数: %d, 值大小: %d bytes\n", 
// 		wct.TestType.String(), wct.RecordCount, wct.ValueSize)
// }

func (wct *WALComparisonTest) setupDBOptions() {
	wct.Options = gorocksdb.NewDefaultOptions()
	wct.Options.SetCreateIfMissing(true)
	
	// === 生产环境内存配置 ===
	// Write Buffer配置 (假设16GB内存系统)
	wct.Options.SetWriteBufferSize(256 * 1024 * 1024)  // 256MB write buffer
	wct.Options.SetMaxWriteBufferNumber(6)              // 6个write buffer
	wct.Options.SetMinWriteBufferNumberToMerge(2)       // 2个buffer合并
	
	// Block Cache配置 (2GB缓存)
	blockCache := gorocksdb.NewLRUCache(2 * 1024 * 1024 * 1024)
	blockOpts := gorocksdb.NewDefaultBlockBasedTableOptions()
	blockOpts.SetBlockCache(blockCache)
	blockOpts.SetBlockSize(16 * 1024) // 16KB block size
	blockOpts.SetCacheIndexAndFilterBlocks(true) // 缓存索引和过滤器
	wct.Options.SetBlockBasedTableFactory(blockOpts)
	
	// === 压缩配置 ===
	// wct.Options.SetCompression(gorocksdb.LZ4Compression)
	
	// === Level配置 ===
	wct.Options.SetNumLevels(7)
	wct.Options.SetMaxBytesForLevelBase(512 * 1024 * 1024) // L1: 512MB
	wct.Options.SetMaxBytesForLevelMultiplier(8)            // 每层8倍增长
	
	// === Compaction配置 ===
	wct.Options.SetLevel0FileNumCompactionTrigger(4)       // 4个文件触发compaction
	wct.Options.SetLevel0SlowdownWritesTrigger(20)         // 20个文件减缓写入
	wct.Options.SetLevel0StopWritesTrigger(36)             // 36个文件停止写入
	
	// 后台线程配置
	// wct.Options.SetMaxBackgroundJobs(8)                    // 8个后台线程
	// wct.Options.SetMaxSubcompactions(4)                    // 4个子compaction
	
	// 文件大小配置
	wct.Options.SetTargetFileSizeBase(128 * 1024 * 1024)   // L1文件128MB
	wct.Options.SetTargetFileSizeMultiplier(2)             // 每层文件大小2倍增长
	
	// === WAL配置 ===
	wct.Options.SetMaxTotalWalSize(2 * 1024 * 1024 * 1024) // 总WAL限制2GB
	
	// === 其他性能配置 ===
	// wct.Options.SetAllowConcurrentMemtableWrite(true)      // 允许并发写入memtable
	// wct.Options.SetEnableWriteThreadAdaptiveYield(true)    // 启用写线程自适应yield
	
	wct.logfToFile("=== 生产环境配置 ===\n")
	wct.logfToFile("测试类型: %s, 记录数: %d, 值大小: %d bytes\n", 
		wct.TestType.String(), wct.RecordCount, wct.ValueSize)
	wct.logfToFile("WriteBufferSize: 256MB\n")
	wct.logfToFile("MaxWriteBufferNumber: 6\n")
	wct.logfToFile("BlockCache: 2GB\n")
	wct.logfToFile("BlockSize: 16KB\n")
	wct.logfToFile("Compression: LZ4\n")
	wct.logfToFile("Level0FileNumCompactionTrigger: 4\n")
	// wct.logfToFile("MaxBackgroundJobs: 8\n")
	wct.logfToFile("MaxTotalWalSize: 2GB\n")
}

// 打开数据库
func (wct *WALComparisonTest) openDB() error {
	var err error
	wct.DB, err = gorocksdb.OpenDb(wct.Options, wct.DBPath)
	if err != nil {
		wct.logfToFile("打开数据库失败: %v\n", err)
		return fmt.Errorf("打开数据库失败: %v", err)
	}
	wct.logfToFile("数据库成功打开: %s\n", wct.DBPath)
	return nil
}

// 关闭数据库
func (wct *WALComparisonTest) closeDB() {
	if wct.DB != nil {
		wct.DB.Close()
		wct.DB = nil
		wct.logfToFile("数据库已关闭\n")
	}
	if wct.Options != nil {
		wct.Options.Destroy()
		wct.Options = nil
	}
}

// 获取WAL文件大小
func (wct *WALComparisonTest) getWALSize() (int64, error) {
	var walFileSize int64
	
	files, err := ioutil.ReadDir(wct.DBPath)
	if err != nil {
		return 0, err
	}
	
	wct.logfToFile("检查目录 %s 中的文件:\n", wct.DBPath)
	for _, file := range files {
		wct.logfToFile("  %s (大小: %d bytes)\n", file.Name(), file.Size())
		if strings.HasSuffix(file.Name(), ".log") {
			walFileSize += file.Size()
		}
	}
	
	wct.logfToFile("WAL文件总大小: %.2f MB\n", float64(walFileSize)/(1024*1024))
	return walFileSize, nil
}

// 写入测试数据
func (wct *WALComparisonTest) writeTestData() (int64, error) {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	
	batchSize := 10000
	keyPrefix := "test_key_"
	
	var value string
	switch wct.TestType {
	case FullValue:
		// 1KB的完整值
		value = strings.Repeat("x", wct.ValueSize)
	case OffsetOnly:
		// 10B的偏移量值（模拟Nezha中的偏移量）
		value = "1234567890" // 正好10字节
	}
	
	wct.logfToFile("开始写入 %d 条记录，每个值大小: %d bytes\n", wct.RecordCount, len(value))
	
	fmt.Printf("写入进度 (%s): ", wct.TestType.String())
	
	totalBatches := wct.RecordCount / batchSize
	for batchNum := 0; batchNum < totalBatches; batchNum++ {
		batch := gorocksdb.NewWriteBatch()
		
		for i := 0; i < batchSize; i++ {
			recordIndex := batchNum*batchSize + i
			key := keyPrefix + strconv.Itoa(recordIndex)
			
			var recordValue string
			if wct.TestType == OffsetOnly {
				// 对于偏移量模式，使用记录索引作为偏移量
				recordValue = fmt.Sprintf("%010d", recordIndex) // 10位数字，前面补0
			} else {
				recordValue = value + strconv.Itoa(recordIndex) // 添加唯一标识
			}
			
			batch.Put([]byte(key), []byte(recordValue))
		}
		
		err := wct.DB.Write(wo, batch)
		batch.Destroy()
		if err != nil {
			return -1,fmt.Errorf("批量写入失败: %v", err)
		}
		
		// 显示进度
		if batchNum%(totalBatches/20) == 0 {
			fmt.Print(".")
		}
	}
	
	// 处理剩余的记录
	remaining := wct.RecordCount % batchSize
	if remaining > 0 {
		batch := gorocksdb.NewWriteBatch()
		for i := 0; i < remaining; i++ {
			recordIndex := totalBatches*batchSize + i
			key := keyPrefix + strconv.Itoa(recordIndex)
			
			var recordValue string
			if wct.TestType == OffsetOnly {
				recordValue = fmt.Sprintf("%010d", recordIndex)
			} else {
				recordValue = value + strconv.Itoa(recordIndex)
			}
			
			batch.Put([]byte(key), []byte(recordValue))
		}
		
		err := wct.DB.Write(wo, batch)
		batch.Destroy()
		if err != nil {
			return -1,fmt.Errorf("写入剩余记录失败: %v", err)
		}
	}
	
	finalWALSize, _ := wct.getWALSize()
	fmt.Printf(" 完成! (WAL大小: %.2f MB)\n", float64(finalWALSize)/(1024*1024))
	wct.logfToFile("数据写入完成，WAL大小: %.2f MB\n", float64(finalWALSize)/(1024*1024))
	
	return finalWALSize, nil
}

// 模拟异常中断
func (wct *WALComparisonTest) simulateCrash() {
	wct.logfToFile("模拟异常中断...\n")
	if wct.DB != nil {
		wct.DB.Close()
		wct.DB = nil
	}
	wct.logfToFile("异常中断完成\n")
}

// 测量恢复性能
func (wct *WALComparisonTest) measureRecovery() (ComparisonResult, error) {
	result := ComparisonResult{
		TestType:    wct.TestType.String(),
		RecordCount: wct.RecordCount,
		ValueSize:   wct.ValueSize,
	}
	
	fmt.Printf("恢复测试 (%s): ", wct.TestType.String())
	wct.logfToFile("开始恢复性能测试...\n")
	
	// 记录恢复前内存
	result.MemoryBefore = getMemoryUsage()
	wct.logfToFile("恢复前内存使用: %.2f MB\n", float64(result.MemoryBefore)/(1024*1024))
	
	// 重新设置选项
	wct.setupDBOptions()
	
	// 启动监控
	done := make(chan bool)
	memChan := wct.monitorMemoryPeak(done)
	cpuChan := wct.monitorCPUPeak(done)
	
	// 开始恢复测试
	startTime := time.Now()
	wct.logfToFile("恢复开始时间: %s\n", startTime.Format("15:04:05.000"))
	
	err := wct.openDB()
	if err != nil {
		close(done)
		return result, err
	}
	
	// 恢复完成
	recoveryTime := time.Since(startTime)
	result.RecoveryTime = recoveryTime
	
	// 停止监控并获取结果
	close(done)
	result.CPUPeak = <-cpuChan
	result.MemoryPeak = <-memChan
	result.MemoryAfter = getMemoryUsage()
	
	// 获取WAL大小
	// result.WALSize, _ = wct.getWALSize()
	
	fmt.Printf("完成 (耗时: %v, 内存峰值: %.2f MB)\n", 
		recoveryTime, float64(result.MemoryPeak)/(1024*1024))
	
	wct.logfToFile("恢复完成 - 耗时: %v\n", recoveryTime)
	wct.logfToFile("内存使用 - 恢复前: %.2f MB, 峰值: %.2f MB, 恢复后: %.2f MB\n",
		float64(result.MemoryBefore)/(1024*1024),
		float64(result.MemoryPeak)/(1024*1024),
		float64(result.MemoryAfter)/(1024*1024))
	wct.logfToFile("CPU峰值估算: %.2f\n", result.CPUPeak)
	
	return result, nil
}

// 验证数据完整性
func (wct *WALComparisonTest) verifyDataIntegrity() bool {
	fmt.Printf("验证数据完整性 (%s): ", wct.TestType.String())
	wct.logfToFile("开始数据完整性验证...\n")
	
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	
	keyPrefix := "test_key_"
	checkCount := 10000 // 检查前10000条记录
	successCount := 0
	
	for i := 0; i < checkCount; i++ {
		key := keyPrefix + strconv.Itoa(i)
		value, err := wct.DB.Get(ro, []byte(key))
		if err != nil {
			wct.logfToFile("读取key %s 失败: %v\n", key, err)
			continue
		}
		
		if value.Size() > 0 {
			// 验证值的正确性
			valueStr := string(value.Data())
			if wct.TestType == OffsetOnly {
				expectedValue := fmt.Sprintf("%010d", i)
				if valueStr == expectedValue {
					successCount++
				}
			} else {
				// 对于完整值，检查是否包含记录索引
				if strings.HasSuffix(valueStr, strconv.Itoa(i)) {
					successCount++
				}
			}
		}
		value.Free()
	}
	
	integrity := successCount == checkCount
	fmt.Printf("%d/%d 验证成功\n", successCount, checkCount)
	wct.logfToFile("数据完整性验证: %d/%d 成功, 完整性: %v\n", successCount, checkCount, integrity)
	
	return integrity
}

// 清理资源
func (wct *WALComparisonTest) cleanup() {
	wct.closeDB()
	
	// 只清理锁文件，保留测试数据
	time.Sleep(100 * time.Millisecond)
	lockFile := filepath.Join(wct.DBPath, "LOCK")
	if _, err := os.Stat(lockFile); err == nil {
		os.Remove(lockFile)
		wct.logfToFile("已清理锁文件\n")
	}
}

// 运行对比测试
func runComparisonTest(testType TestType, recordCount int, valueSize int, logFile *os.File) ComparisonResult {
	test := NewWALComparisonTest(testType, recordCount, valueSize)
	test.LogFile = logFile
	
	defer test.cleanup()
	
	// 检查测试目录
	if _, err := os.Stat(test.DBPath); err == nil {
		fmt.Printf("使用现有目录: %s\n", test.DBPath)
		test.logfToFile("使用现有目录: %s\n", test.DBPath)
	} else {
		fmt.Printf("创建新目录: %s\n", test.DBPath)
		test.logfToFile("创建新目录: %s\n", test.DBPath)
	}
	
	// 1. 配置数据库
	test.setupDBOptions()
	
	// 2. 打开数据库
	err := test.openDB()
	if err != nil {
		log.Printf("打开数据库失败: %v", err)
		return ComparisonResult{}
	}
	
	// 3. 写入测试数据
	walSize, err := test.writeTestData()
	if err != nil {
		log.Printf("写入数据失败: %v", err)
		return ComparisonResult{}
	}

	// 4. 模拟异常中断
	test.simulateCrash()
	time.Sleep(500 * time.Millisecond)
	
	// 5. 测量恢复性能
	result, err := test.measureRecovery()
	if err != nil {
		log.Printf("恢复测试失败: %v", err)
		return ComparisonResult{}
	}
	
	// 6. 验证数据完整性
	result.WALSizeAtWrite = walSize  // 保存写入时的WAL大小
	result.DataIntegrity = test.verifyDataIntegrity()
	result.WALSize = result.WALSizeAtWrite  // 使用写入时记录的大小
	
	return result
}

func main() {
	fmt.Println("=== Nezha 键值分离 WAL 恢复性能对比测试 ===\n")
	
	// 创建日志文件
	logFileName := fmt.Sprintf("comparison_test_results_%s.txt", time.Now().Format("2006-01-02_15-04-05"))
	logFile, err := os.Create(logFileName)
	if err != nil {
		log.Fatalf("无法创建日志文件: %v", err)
	}
	defer logFile.Close()
	
	fmt.Printf("详细日志将保存到: %s\n", logFileName)
	fmt.Printf("测试文件将保留在对应目录中\n\n")
	
	logFile.WriteString("=== Nezha 键值分离 WAL 恢复性能对比测试 ===\n")
	logFile.WriteString(fmt.Sprintf("测试时间: %s\n", time.Now().Format("2006-01-02 15:04:05")))
	logFile.WriteString("测试配置: 1,000,000 条记录\n")
	logFile.WriteString("对比: 传统方式(1KB值) vs 键值分离方式(10B偏移量)\n\n")
	
	var results []ComparisonResult
	
	// 测试1: 传统方式 - 1KB完整值
	fmt.Println("=== 测试 1/2: 传统方式 (1KB完整值) ===")
	logFile.WriteString("=== 测试 1: 传统方式 (1KB完整值) ===\n")
	result1 := runComparisonTest(FullValue, 1000000, 1024, logFile)
	results = append(results, result1)
	fmt.Printf("测试完成!\n\n")
	
	// 等待系统稳定
	time.Sleep(3 * time.Second)
	
	// 测试2: 键值分离方式 - 10B偏移量
	fmt.Println("=== 测试 2/2: 键值分离方式 (10B偏移量) ===")
	logFile.WriteString("=== 测试 2: 键值分离方式 (10B偏移量) ===\n")
	result2 := runComparisonTest(OffsetOnly, 1000000, 10, logFile)
	results = append(results, result2)
	fmt.Printf("测试完成!\n\n")
	
	// 生成对比报告
	fmt.Println("=== 性能对比结果 ===")
	fmt.Printf("%-15s %-12s %-15s %-15s %-15s %-12s\n", 
		"测试方式", "记录数", "恢复时间", "WAL大小", "内存峰值", "数据完整性")
	fmt.Printf("%-15s %-12s %-15s %-15s %-15s %-12s\n", 
		"", "(万)", "(ms)", "(MB)", "(MB)", "")
	fmt.Println(strings.Repeat("-", 90))
	
	// 输出到日志文件
	logFile.WriteString("=== 性能对比结果 ===\n")
	logFile.WriteString(fmt.Sprintf("%-15s %-12s %-15s %-15s %-15s %-12s\n", 
		"测试方式", "记录数", "恢复时间", "WAL大小", "内存峰值", "数据完整性"))
	logFile.WriteString(strings.Repeat("-", 90) + "\n")
	
	for _, result := range results {
		integrity := "失败"
		if result.DataIntegrity {
			integrity = "成功"
		}
		
		line := fmt.Sprintf("%-15s %-12d %-15d %-15.2f %-15.2f %-12s\n",
			result.TestType,
			result.RecordCount/10000,
			result.RecoveryTime.Milliseconds(),
			float64(result.WALSize)/(1024*1024),
			float64(result.MemoryPeak)/(1024*1024),
			integrity)
		
		fmt.Print(line)
		logFile.WriteString(line)
	}
	
	// 计算性能提升
	if len(results) == 2 {
		timeImprovement := float64(results[0].RecoveryTime.Milliseconds()-results[1].RecoveryTime.Milliseconds()) / 
			float64(results[0].RecoveryTime.Milliseconds()) * 100
		walReduction := float64(results[0].WALSize-results[1].WALSize) / float64(results[0].WALSize) * 100
		memoryReduction := float64(results[0].MemoryPeak-results[1].MemoryPeak) / float64(results[0].MemoryPeak) * 100
		
		fmt.Printf("\n=== 键值分离优化效果 ===\n")
		fmt.Printf("恢复时间优化: %.1f%%\n", timeImprovement)
		fmt.Printf("WAL大小减少: %.1f%%\n", walReduction)
		fmt.Printf("内存使用减少: %.1f%%\n", memoryReduction)
		
		logFile.WriteString("\n=== 键值分离优化效果 ===\n")
		logFile.WriteString(fmt.Sprintf("恢复时间优化: %.1f%%\n", timeImprovement))
		logFile.WriteString(fmt.Sprintf("WAL大小减少: %.1f%%\n", walReduction))
		logFile.WriteString(fmt.Sprintf("内存使用减少: %.1f%%\n", memoryReduction))
	}
	
	fmt.Printf("\n测试完成！详细日志已保存到: %s\n", logFileName)
	
	// 显示测试数据位置
	fmt.Println("\n=== 测试数据位置 ===")
	fmt.Printf("传统方式数据: ./test_comparison_传统方式_1000k_records/\n")
	fmt.Printf("键值分离数据: ./test_comparison_键值分离_1000k_records/\n")
	fmt.Printf("可以使用 du -h 查看目录大小差异\n")
}