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

// 只输出到日志文件
func (wt *WALTest) logfToFile(format string, args ...interface{}) {
	if wt.LogFile != nil {
		message := fmt.Sprintf(format, args...)
		wt.LogFile.WriteString(message)
		wt.LogFile.Sync()
	}
}

// 同时输出到控制台和文件（保留关键信息用）
func (wt *WALTest) logf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	fmt.Print(message)
	if wt.LogFile != nil {
		wt.LogFile.WriteString(message)
		wt.LogFile.Sync()
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
	
	wt.logfToFile("配置WAL目标大小: %d MB\n", wt.TargetSize/(1024*1024))
	wt.logfToFile("WriteBufferSize: %d bytes\n", int(wt.TargetSize * 2))
	wt.logfToFile("MaxWriteBufferNumber: 3\n")
	wt.logfToFile("DisableAutoCompactions: true\n")
	wt.logfToFile("Level0FileNumCompactionTrigger: 1000\n")
}

// 打开数据库
func (wt *WALTest) openDB() error {
	var err error
	wt.DB, err = gorocksdb.OpenDb(wt.Options, wt.DBPath)
	if err != nil {
		wt.logfToFile("打开数据库失败: %v\n", err)
		return fmt.Errorf("打开数据库失败: %v", err)
	}
	wt.logfToFile("数据库成功打开: %s\n", wt.DBPath)
	return nil
}

// 关闭数据库
func (wt *WALTest) closeDB() {
	if wt.DB != nil {
		wt.DB.Close()
		wt.DB = nil
		wt.logfToFile("数据库已关闭\n")
	}
	if wt.Options != nil {
		wt.Options.Destroy()
		wt.Options = nil
		wt.logfToFile("数据库选项已销毁\n")
	}
}

// 获取WAL文件大小 (详细信息只输出到日志文件)
func (wt *WALTest) getWALSize() (int64, error) {
	var walFileSize int64
	
	walDir := wt.DBPath
	files, err := ioutil.ReadDir(walDir)
	if err != nil {
		wt.logfToFile("读取目录失败: %v\n", err)
		return 0, err
	}
	
	// 详细信息只输出到日志文件
	wt.logfToFile("检查目录 %s 中的文件:\n", walDir)
	for _, file := range files {
		wt.logfToFile("  %s (大小: %d bytes)\n", file.Name(), file.Size())
		if strings.HasSuffix(file.Name(), ".log") {
			walFileSize += file.Size()
		}
	}
	
	wt.logfToFile("纯WAL文件大小: %.2f MB\n", float64(walFileSize)/(1024*1024))
	return walFileSize, nil
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
	
	wt.logfToFile("开始写入数据，目标WAL大小: %d MB\n", wt.TargetSize/(1024*1024))
	wt.logfToFile("批量大小: %d, 每个值大小: %d bytes\n", batchSize, valueSize)
	
	fmt.Print("写入进度: ")
	
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
			wt.logfToFile("批量写入数据失败: %v\n", err)
			return recordCount, fmt.Errorf("批量写入数据失败: %v", err)
		}
		
		// 检查WAL大小
		walSize, err := wt.getWALSize()
		if err != nil {
			wt.logfToFile("获取WAL大小失败: %v\n", err)
			return recordCount, fmt.Errorf("获取WAL大小失败: %v", err)
		}
		
		if recordCount%10000 == 0 {
			// 控制台只显示简单进度
			fmt.Print(".")
			// 详细信息只写入日志文件
			wt.logfToFile("已写入 %d 条记录，当前WAL大小: %.2f MB\n", 
				recordCount, float64(walSize)/(1024*1024))
		}
		
		// 达到目标大小后再多写一批数据确保有未提交的WAL
		if walSize >= int64(wt.TargetSize) {
			fmt.Print(" 完成!")
			wt.logfToFile("达到目标WAL大小，继续写入一批数据...\n")
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
				wt.logfToFile("写入数据失败: %v\n", err)
				return recordCount, fmt.Errorf("写入数据失败: %v", err)
			}
			break
		}
	}
	
	finalWALSize, _ := wt.getWALSize()
	fmt.Printf("\n写入完成: %d 条记录，WAL大小: %.2f MB\n", 
		recordCount, float64(finalWALSize)/(1024*1024))
	wt.logfToFile("写入完成，共写入 %d 条记录，最终WAL大小: %.2f MB\n", 
		recordCount, float64(finalWALSize)/(1024*1024))
	
	return recordCount, nil
}

// 模拟异常中断
func (wt *WALTest) simulateCrash() error {
	wt.logfToFile("开始模拟异常中断...\n")
	
	// 强制关闭数据库但不进行正常的cleanup，模拟异常退出
	if wt.DB != nil {
		wt.DB.Close() // 需要关闭以释放锁，但不做其他cleanup
		wt.DB = nil
		wt.logfToFile("数据库连接已强制关闭\n")
	}
	
	wt.logfToFile("异常中断模拟完成\n")
	return nil
}

// 测量恢复性能
func (wt *WALTest) measureRecovery() (time.Duration, error) {
	fmt.Print("恢复中...")
	wt.logfToFile("开始恢复测试...\n")
	
	// 重新设置选项
	wt.setupDBOptions()
	
	// 记录开始时间
	startTime := time.Now()
	wt.logfToFile("恢复开始时间: %s\n", startTime.Format("15:04:05.000"))
	
	// 重新打开数据库，这会触发WAL恢复
	err := wt.openDB()
	if err != nil {
		wt.logfToFile("恢复失败: %v\n", err)
		return 0, err
	}
	
	// 记录恢复完成时间
	recoveryTime := time.Since(startTime)
	endTime := time.Now()
	
	fmt.Printf(" 完成 (耗时: %v)\n", recoveryTime)
	wt.logfToFile("恢复完成时间: %s\n", endTime.Format("15:04:05.000"))
	wt.logfToFile("数据库恢复完成，总耗时: %v\n", recoveryTime)
	
	return recoveryTime, nil
}

// 验证数据完整性
func (wt *WALTest) verifyDataIntegrity(expectedRecords int) (int, bool) {
	fmt.Print("验证数据完整性...")
	wt.logfToFile("开始数据完整性验证...\n")
	
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	
	keyPrefix := "test_key_"
	recoveredCount := 0
	
	// 检查前1000条记录作为样本
	checkCount := 1000
	if expectedRecords < checkCount {
		checkCount = expectedRecords
	}
	
	wt.logfToFile("将检查前 %d 条记录作为样本\n", checkCount)
	
	for i := 0; i < checkCount; i++ {
		key := keyPrefix + strconv.Itoa(i)
		value, err := wt.DB.Get(ro, []byte(key))
		if err != nil {
			wt.logfToFile("读取key %s 失败: %v\n", key, err)
			continue
		}
		if value.Size() > 0 {
			recoveredCount++
		}
		value.Free()
	}
	
	integrity := recoveredCount == checkCount
	fmt.Printf(" %d/%d 条记录成功\n", recoveredCount, checkCount)
	wt.logfToFile("数据完整性验证结果: %d/%d 条记录恢复成功, 完整性: %v\n", 
		recoveredCount, checkCount, integrity)
	
	return recoveredCount, integrity
}

// 获取内存使用情况
func getMemoryUsage() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc
}

// 清理测试环境 (保留测试文件，只清理锁)
func (wt *WALTest) cleanup() error {
	wt.closeDB()
	
	// 添加延迟确保文件系统完成操作
	time.Sleep(100 * time.Millisecond)
	
	// 只删除锁文件，保留其他测试数据
	lockFile := filepath.Join(wt.DBPath, "LOCK")
	if _, err := os.Stat(lockFile); err == nil {
		wt.logfToFile("删除锁文件: %s\n", lockFile)
		os.Remove(lockFile)
	}
	
	wt.logfToFile("cleanup完成，测试文件已保留在: %s\n", wt.DBPath)
	return nil
}

// 运行单个WAL大小测试
func runWALSizeTest(targetSize uint64, logFile *os.File) RecoveryResult {
	result := RecoveryResult{TargetWALSize: targetSize}
	
	dbPath := fmt.Sprintf("./test_wal_%dmb", targetSize/(1024*1024))
	walTest := NewWALTest(dbPath, targetSize)
	walTest.LogFile = logFile // 设置日志文件
	
	// 检查是否存在测试目录，不删除已有数据
	if _, err := os.Stat(dbPath); err == nil {
		fmt.Printf("使用现有测试目录: %s\n", dbPath)
		walTest.logfToFile("使用现有测试目录: %s\n", dbPath)
	} else {
		fmt.Printf("创建新的测试目录: %s\n", dbPath)
		walTest.logfToFile("创建新的测试目录: %s\n", dbPath)
	}
	// os.RemoveAll(dbPath) // 确保目录为空
	
	time.Sleep(100 * time.Millisecond) // 等待文件系统操作完成
	
	defer walTest.cleanup()
	
	// 1. 设置数据库配置
	walTest.setupDBOptions()
	
	// 2. 打开数据库
	err := walTest.openDB()
	if err != nil {
		log.Printf("测试 %d MB WAL 失败: %v", targetSize/(1024*1024), err)
		walTest.logfToFile("测试失败: %v\n", err)
		return result
	}
	
	// 3. 写入数据直到达到目标WAL大小
	recordsWritten, err := walTest.writeDataUntilTarget()
	if err != nil {
		log.Printf("写入数据失败: %v", err)
		walTest.logfToFile("写入数据失败: %v\n", err)
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
		walTest.logfToFile("恢复测试失败: %v\n", err)
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
	
	fmt.Printf("详细日志将保存到: %s\n", logFileName)
	fmt.Printf("测试文件将保留，不会自动删除\n\n")
	
	logFile.WriteString(fmt.Sprintf("=== RocksDB WAL 大小恢复性能测试 ===\n"))
	logFile.WriteString(fmt.Sprintf("测试时间: %s\n", time.Now().Format("2006-01-02 15:04:05")))
	logFile.WriteString(fmt.Sprintf("测试文件将保留在对应目录中\n\n"))
	
	// 测试不同的WAL大小
	walSizes := []uint64{
		1 * 1024 * 1024,    // 1MB
		10 * 1024 * 1024,   // 10MB
		50 * 1024 * 1024,   // 50MB
		100 * 1024 * 1024,  // 100MB
	}
	
	var results []RecoveryResult
	
	for i, size := range walSizes {
		fmt.Printf("[%d/%d] 测试 WAL 大小: %d MB\n", i+1, len(walSizes), size/(1024*1024))
		logFile.WriteString(fmt.Sprintf("--- 测试 WAL 大小: %d MB ---\n", size/(1024*1024)))
		
		result := runWALSizeTest(size, logFile)
		results = append(results, result)
		
		fmt.Printf("测试完成!\n\n")
		logFile.WriteString("测试完成!\n\n")
		time.Sleep(1 * time.Second) // 等待系统稳定
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
	fmt.Printf("3. 查看目录内容: ls -la ./test_wal_100mb/\n")
	fmt.Printf("4. 查看WAL文件大小: du -h ./test_wal_100mb/*.log\n")
	fmt.Printf("5. 如果安装了RocksDB ldb工具:\n")
	fmt.Printf("   ldb --db=./test_wal_100mb dump     # 查看所有数据\n")
	fmt.Printf("   ldb --db=./test_wal_100mb stats    # 查看统计信息\n")
	fmt.Printf("   ldb --db=./test_wal_100mb scan     # 扫描所有键\n\n")
}