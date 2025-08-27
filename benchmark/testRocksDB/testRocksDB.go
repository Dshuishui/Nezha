package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	// "path/filepath"
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
	
	fmt.Printf("配置WAL目标大小: %d MB\n", wt.TargetSize/(1024*1024))
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

// 获取WAL文件大小
func (wt *WALTest) getWALSize() (int64, error) {
	var totalSize int64
	
	walDir := wt.DBPath
	files, err := ioutil.ReadDir(walDir)
	if err != nil {
		return 0, err
	}
	
	for _, file := range files {
		// RocksDB的WAL文件通常以.log结尾
		if strings.HasSuffix(file.Name(), ".log") {
			totalSize += file.Size()
		}
	}
	
	return totalSize, nil
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
	
	fmt.Printf("开始写入数据，目标WAL大小: %d MB\n", wt.TargetSize/(1024*1024))
	
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
		}
		
		// 达到目标大小后再多写一批数据确保有未提交的WAL
		if walSize >= int64(wt.TargetSize) {
			fmt.Printf("达到目标WAL大小，继续写入一批数据...\n")
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
	fmt.Printf("写入完成，共写入 %d 条记录，最终WAL大小: %.2f MB\n", 
		recordCount, float64(finalWALSize)/(1024*1024))
	
	return recordCount, nil
}

// 模拟异常中断
func (wt *WALTest) simulateCrash() error {
	fmt.Println("模拟异常中断...")
	
	// 不调用正常的Close()，直接强制关闭
	if wt.DB != nil {
		// 这里我们不调用Close()来模拟异常退出
		wt.DB = nil
	}
	
	// 在实际测试中，你可能需要在这里用另一个进程来kill当前进程
	// 这里我们简单地表示异常退出的状态
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
	return os.RemoveAll(wt.DBPath)
}

// 运行单个WAL大小测试
func runWALSizeTest(targetSize uint64) RecoveryResult {
	result := RecoveryResult{TargetWALSize: targetSize}
	
	dbPath := fmt.Sprintf("./test_wal_%dmb", targetSize/(1024*1024))
	walTest := NewWALTest(dbPath, targetSize)
	
	// 清理之前的测试数据
	os.RemoveAll(dbPath)
	
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
	
	// 测试不同的WAL大小
	walSizes := []uint64{
		1 * 1024 * 1024,    // 1MB
		10 * 1024 * 1024,   // 10MB
		50 * 1024 * 1024,   // 50MB
		100 * 1024 * 1024,  // 100MB
	}
	
	var results []RecoveryResult
	
	for _, size := range walSizes {
		fmt.Printf("\n--- 测试 WAL 大小: %d MB ---\n", size/(1024*1024))
		
		result := runWALSizeTest(size)
		results = append(results, result)
		
		fmt.Printf("测试完成!\n")
		time.Sleep(2 * time.Second) // 等待系统稳定
	}
	
	// 打印测试结果汇总
	fmt.Printf("\n=== 测试结果汇总 ===\n")
	fmt.Printf("%-10s %-12s %-12s %-10s %-12s %-10s\n", 
		"目标WAL", "实际WAL", "恢复时间", "写入记录", "恢复记录", "数据完整性")
	fmt.Printf("%-10s %-12s %-12s %-10s %-12s %-10s\n", 
		"(MB)", "(MB)", "(ms)", "数量", "数量", "状态")
	fmt.Println(strings.Repeat("-", 80))
	
	for _, result := range results {
		integrity := "失败"
		if result.DataIntegrity {
			integrity = "成功"
		}
		
		fmt.Printf("%-10d %-12.2f %-12d %-10d %-12d %-10s\n",
			result.TargetWALSize/(1024*1024),
			float64(result.ActualWALSize)/(1024*1024),
			result.RecoveryTime.Milliseconds(),
			result.RecordsWritten,
			result.RecordsRecovered,
			integrity)
	}
	
	fmt.Println("\n测试完成!")
}