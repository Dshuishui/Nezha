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
	// "syscall"
	"time"

	"github.com/tecbot/gorocksdb"
)

type WALTest struct {
	DBPath     string
	TargetSize uint64
	DB         *gorocksdb.DB
	Options    *gorocksdb.Options
	LogFile    *os.File  // 添加日志文件
}

type RecoveryResult struct {
	TargetWALSize     uint64
	ActualWALSize     int64
	RecoveryTime      time.Duration
	DataIntegrity     bool
	RecordsWritten    int
	RecordsRecovered  int
	MemoryUsage       uint64
}

func NewWALTest(dbPath string, targetSize uint64) *WALTest {
	return &WALTest{
		DBPath:     dbPath,
		TargetSize: targetSize,
	}
}

// 配置RocksDB选项，控制WAL大小
func (wt *WALTest) setupDBOptions() {
	wt.Options = gorocksdb.NewDefaultOptions()
	wt.Options.SetCreateIfMissing(true)
	
	// 调整写入缓冲区，避免过早flush到SST，让数据保留在WAL中
	wt.Options.SetWriteBufferSize(int(wt.TargetSize * 2)) // 设置为目标WAL大小的2倍
	wt.Options.SetMaxWriteBufferNumber(3)
	wt.Options.SetMinWriteBufferNumberToMerge(1)
	
	// 禁用自动压缩，避免影响WAL测试
	wt.Options.SetDisableAutoCompactions(true)
	
	// 设置较大的Level 0文件数量阈值，延迟compaction
	wt.Options.SetLevel0FileNumCompactionTrigger(1000)
	
	wt.logf("配置WAL目标大小: %d MB\n", wt.TargetSize/(1024*1024))
}

// 打开数据库
func (wt *WALTest) openDB() error {
	var err error
	wt.DB, err = gorocksdb.OpenDb(wt.Options, wt.DBPath)
	if err != nil {
		return fmt.Errorf("打开数据库失败: %v", err)
	}
	return nil
}

// 关闭数据库
func (wt *WALTest) closeDB() {
	if wt.DB != nil {
		wt.DB.Close()
		wt.DB = nil
	}
	if wt.Options != nil {
		wt.Options.Destroy()
		wt.Options = nil
	}
}

// 日志记录函数
func (wt *WALTest) logf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	// 同时输出到控制台和文件
	fmt.Print(message)
	if wt.LogFile != nil {
		wt.LogFile.WriteString(message)
		wt.LogFile.Sync() // 确保立即写入
	}
}

func (wt *WALTest) logln(args ...interface{}) {
	message := fmt.Sprintln(args...)
	fmt.Print(message)
	if wt.LogFile != nil {
		wt.LogFile.WriteString(message)
		wt.LogFile.Sync()
	}
}

// 获取WAL文件大小 (简化输出版本)
func (wt *WALTest) getWALSize() (int64, error) {
	var totalSize int64
	var walFileSize int64
	
	walDir := wt.DBPath
	files, err := ioutil.ReadDir(walDir)
	if err != nil {
		return 0, err
	}
	
	// 只记录详细信息到日志文件，控制台显示简化版本
	if wt.LogFile != nil {
		wt.logf("检查目录 %s 中的文件:\n", walDir)
		for _, file := range files {
			wt.logf("  %s (大小: %d bytes)\n", file.Name(), file.Size())
		}
	}
	
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".log") {
			walFileSize += file.Size()
		}
		// RocksDB的WAL文件通常以.log结尾，或者是CURRENT、MANIFEST等文件
		if strings.HasSuffix(file.Name(), ".log") || 
		   strings.Contains(file.Name(), "CURRENT") ||
		   strings.Contains(file.Name(), "MANIFEST") {
			totalSize += file.Size()
		}
	}
	
	return walFileSize, nil // 返回纯WAL文件大小
}

// 写入测试数据直到WAL达到目标大小
func (wt *WALTest) writeDataUntilTarget() (int, error) {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	
	recordCount := 0
	batchSize := 1000
	keyPrefix := "test_key_"
	valueSize := 1024 // 1KB per value
	value := strings.Repeat("x", valueSize)
	
	wt.logf("开始写入数据，目标WAL大小: %d MB\n", wt.TargetSize/(1024*1024))
	
	for {
		// 使用批量写入来提高效率并控制WAL大小
		batch := gorocksdb.NewWriteBatch()
		
		for i := 0; i < batchSize; i++ {
			key := keyPrefix + strconv.Itoa(recordCount)
			batch.Put([]byte(key), []byte(value+strconv.Itoa(recordCount)))
			recordCount++
		}
		
		// 执行批量写入
		err := wt.DB.Write(wo, batch)
		batch.Destroy()
		if err != nil {
			return recordCount, fmt.Errorf("批量写入数据失败: %v", err)
		}
		
		// 检查WAL大小
		walSize, err := wt.getWALSize()
		if err != nil {
			return recordCount, fmt.Errorf("获取WAL大小失败: %v", err)
		}
		
		if recordCount%10000 == 0 {
			fmt.Printf("已写入 %d 条记录，当前WAL大小: %.2f MB\n", 
				recordCount, float64(walSize)/(1024*1024))
			wt.logf("已写入 %d 条记录，当前WAL大小: %.2f MB\n", 
				recordCount, float64(walSize)/(1024*1024))
		}
		
		// 达到目标大小后再多写一批数据确保有未提交的WAL
		if walSize >= int64(wt.TargetSize) {
			fmt.Println("达到目标WAL大小，继续写入一批数据...")
			wt.logln("达到目标WAL大小，继续写入一批数据...")
			// 再写入一批数据
			batch := gorocksdb.NewWriteBatch()
			for i := 0; i < batchSize; i++ {
				key := keyPrefix + strconv.Itoa(recordCount)
				batch.Put([]byte(key), []byte(value+strconv.Itoa(recordCount)))
				recordCount++
			}
			err := wt.DB.Write(wo, batch)
			batch.Destroy()
			if err != nil {
				return recordCount, fmt.Errorf("写入数据失败: %v", err)
			}
			break
		}
	}
	
	finalWALSize, _ := wt.getWALSize()
	message := fmt.Sprintf("写入完成，共写入 %d 条记录，最终WAL大小: %.2f MB\n", 
		recordCount, float64(finalWALSize)/(1024*1024))
	fmt.Print(message)
	wt.logf(message)
	
	return recordCount, nil
}

// 模拟异常中断
func (wt *WALTest) simulateCrash() error {
	fmt.Println("模拟异常中断...")
	
	// 强制关闭数据库但不进行正常的cleanup，模拟异常退出
	if wt.DB != nil {
		wt.DB.Close() // 需要关闭以释放锁，但不做其他cleanup
		wt.DB = nil
	}
	
	fmt.Println("数据库异常关闭完成")
	return nil
}

// 测量恢复性能
func (wt *WALTest) measureRecovery() (time.Duration, error) {
	fmt.Println("开始恢复测试...")
	
	// 重新设置选项
	wt.setupDBOptions()
	
	// 记录开始时间
	startTime := time.Now()
	
	// 重新打开数据库，这会触发WAL恢复
	err := wt.openDB()
	if err != nil {
		return 0, err
	}
	
	// 记录恢复完成时间
	recoveryTime := time.Since(startTime)
	
	fmt.Printf("数据库恢复完成，耗时: %v\n", recoveryTime)
	
	return recoveryTime, nil
}

// 验证数据完整性
func (wt *WALTest) verifyDataIntegrity(expectedRecords int) (int, bool) {
	fmt.Println("验证数据完整性...")
	
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	
	keyPrefix := "test_key_"
	recoveredCount := 0
	
	// 检查前1000条记录作为样本
	checkCount := 1000
	if expectedRecords < checkCount {
		checkCount = expectedRecords
	}
	
	for i := 0; i < checkCount; i++ {
		key := keyPrefix + strconv.Itoa(i)
		value, err := wt.DB.Get(ro, []byte(key))
		if err != nil {
			fmt.Printf("读取key %s 失败: %v\n", key, err)
			continue
		}
		if value.Size() > 0 {
			recoveredCount++
		}
		value.Free()
	}
	
	integrity := recoveredCount == checkCount
	fmt.Printf("数据完整性验证: %d/%d 条记录恢复成功\n", recoveredCount, checkCount)
	
	return recoveredCount, integrity
}

// 获取内存使用情况
func getMemoryUsage() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc
}

// 清理测试环境
func (wt *WALTest) cleanup() error {
	wt.closeDB()
	
	// 添加延迟确保文件系统完成操作
	time.Sleep(100 * time.Millisecond)
	
	// 尝试删除锁文件
	lockFile := filepath.Join(wt.DBPath, "LOCK")
	if _, err := os.Stat(lockFile); err == nil {
		fmt.Printf("删除锁文件: %s\n", lockFile)
		os.Remove(lockFile)
	}
	
	return os.RemoveAll(wt.DBPath)
}

// 运行单个WAL大小测试
func runWALSizeTest(targetSize uint64, logFile *os.File) RecoveryResult {
	result := RecoveryResult{TargetWALSize: targetSize}
	
	dbPath := fmt.Sprintf("./test_wal_%dmb", targetSize/(1024*1024))
	walTest := NewWALTest(dbPath, targetSize)
	walTest.LogFile = logFile // 设置日志文件
	
	// 清理之前的测试数据
	os.RemoveAll(dbPath)
	time.Sleep(100 * time.Millisecond) // 等待文件系统操作完成
	
	defer walTest.cleanup()
	
	// 1. 设置数据库配置
	walTest.setupDBOptions()
	
	// 2. 打开数据库
	err := walTest.openDB()
	if err != nil {
		log.Printf("测试 %d MB WAL 失败: %v", targetSize/(1024*1024), err)
		return result
	}
	
	// 3. 写入数据直到达到目标WAL大小
	recordsWritten, err := walTest.writeDataUntilTarget()
	if err != nil {
		log.Printf("写入数据失败: %v", err)
		return result
	}
	result.RecordsWritten = recordsWritten
	
	// 4. 获取实际WAL大小
	actualWALSize, _ := walTest.getWALSize()
	result.ActualWALSize = actualWALSize
	
	// 5. 模拟异常中断
	walTest.simulateCrash()
	
	// 等待一段时间确保锁释放
	time.Sleep(500 * time.Millisecond)
	
	// 6. 测量恢复性能
	memBefore := getMemoryUsage()
	recoveryTime, err := walTest.measureRecovery()
	if err != nil {
		log.Printf("恢复测试失败: %v", err)
		return result
	}
	result.RecoveryTime = recoveryTime
	result.MemoryUsage = getMemoryUsage() - memBefore
	
	// 7. 验证数据完整性
	recoveredCount, integrity := walTest.verifyDataIntegrity(recordsWritten)
	result.RecordsRecovered = recoveredCount
	result.DataIntegrity = integrity
	
	return result
}

func main() {
	fmt.Println("=== RocksDB WAL 大小恢复性能测试 ===\n")
	
	// 创建日志文件
	logFileName := fmt.Sprintf("wal_test_results_%s.txt", time.Now().Format("2006-01-02_15-04-05"))
	logFile, err := os.Create(logFileName)
	if err != nil {
		log.Fatalf("无法创建日志文件: %v", err)
	}
	defer logFile.Close()
	
	fmt.Printf("详细日志将保存到: %s\n\n", logFileName)
	logFile.WriteString(fmt.Sprintf("=== RocksDB WAL 大小恢复性能测试 ===\n"))
	logFile.WriteString(fmt.Sprintf("测试时间: %s\n\n", time.Now().Format("2006-01-02 15:04:05")))
	
	// 测试不同的WAL大小
	walSizes := []uint64{
		1 * 1024 * 1024,    // 1MB
		10 * 1024 * 1024,   // 10MB
		50 * 1024 * 1024,   // 50MB
		100 * 1024 * 1024,  // 100MB
	}
	
	var results []RecoveryResult
	
	for _, size := range walSizes {
		fmt.Printf("--- 测试 WAL 大小: %d MB ---\n", size/(1024*1024))
		logFile.WriteString(fmt.Sprintf("--- 测试 WAL 大小: %d MB ---\n", size/(1024*1024)))
		
		result := runWALSizeTest(size, logFile)
		results = append(results, result)
		
		fmt.Printf("测试完成!\n\n")
		logFile.WriteString("测试完成!\n\n")
		time.Sleep(2 * time.Second) // 等待系统稳定
	}
	
	// 打印测试结果汇总
	fmt.Printf("=== 测试结果汇总 ===\n")
	fmt.Printf("%-10s %-12s %-12s %-10s %-12s %-10s\n", 
		"目标WAL", "实际WAL", "恢复时间", "写入记录", "恢复记录", "数据完整性")
	fmt.Printf("%-10s %-12s %-12s %-10s %-12s %-10s\n", 
		"(MB)", "(MB)", "(ms)", "数量", "数量", "状态")
	fmt.Println(strings.Repeat("-", 80))
	
	// 同时写入日志文件
	logFile.WriteString("=== 测试结果汇总 ===\n")
	logFile.WriteString(fmt.Sprintf("%-10s %-12s %-12s %-10s %-12s %-10s\n", 
		"目标WAL", "实际WAL", "恢复时间", "写入记录", "恢复记录", "数据完整性"))
	logFile.WriteString(fmt.Sprintf("%-10s %-12s %-12s %-10s %-12s %-10s\n", 
		"(MB)", "(MB)", "(ms)", "数量", "数量", "状态"))
	logFile.WriteString(strings.Repeat("-", 80) + "\n")
	
	for _, result := range results {
		integrity := "失败"
		if result.DataIntegrity {
			integrity = "成功"
		}
		
		resultLine := fmt.Sprintf("%-10d %-12.2f %-12d %-10d %-12d %-10s\n",
			result.TargetWALSize/(1024*1024),
			float64(result.ActualWALSize)/(1024*1024),
			result.RecoveryTime.Milliseconds(),
			result.RecordsWritten,
			result.RecordsRecovered,
			integrity)
		
		fmt.Print(resultLine)
		logFile.WriteString(resultLine)
	}
	
	fmt.Printf("\n测试完成！详细日志已保存到: %s\n", logFileName)
	
	// 显示如何查看WAL数据的说明
	fmt.Println("\n=== WAL数据查看说明 ===")
	fmt.Printf("1. 测试数据库文件位置: ./test_wal_XXmb/ 目录\n")
	fmt.Printf("2. WAL文件通常命名为: 000003.log (数字可能不同)\n")
	fmt.Printf("3. 你可以使用RocksDB的ldb工具查看WAL内容:\n")
	fmt.Printf("   ldb --db=./test_wal_100mb dump\n")
	fmt.Printf("4. 或者查看数据库统计信息:\n")
	fmt.Printf("   ldb --db=./test_wal_100mb stats\n")
	fmt.Printf("5. 查看所有键:\n")
	fmt.Printf("   ldb --db=./test_wal_100mb scan\n\n")
}