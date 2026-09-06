// Read paths over the store, the value log and the sorted files produced by GC.

package kvstore

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"gitee.com/dong-shuishui/FlexSync/api/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/raft"
	"github.com/edsrzf/mmap-go"
	"github.com/linxGnu/grocksdb"
)

func (kvs *KVServer) anotherGCScan(startKey, endKey string) (map[string]string, error) {
	var wg sync.WaitGroup
	wg.Add(2)

	oldChan := make(chan scanResult, 1)
	sortedChan := make(chan scanResult, 1)
	newChan := make(chan scanResult, 1)

	if !kvs.anotherStartGC {
		// GC前：并行查询上一轮新文件，上一轮排序文件
		go func() {
			defer wg.Done()
			result, err := kvs.scanFromSortedFile(startKey, endKey, kvs.lastSortedFileIndex)
			sortedChan <- scanResult{data: result, err: err}
		}()

		// 查询新文件
		go func() {
			defer wg.Done()
			result := kvs.StartScan_opt(&kvrpc.ScanRangeRequest{
				StartKey: startKey,
				EndKey:   endKey,
			}, kvs.persister, kvs.currentLog)
			oldChan <- scanResultOf(result)
		}()

		wg.Wait()
		close(sortedChan)
		close(oldChan)

		sortedResult := <-sortedChan
		oldResult := <-oldChan

		if sortedResult.err != nil {
			return nil, fmt.Errorf("error scanning sorted file: %v", sortedResult.err)
		}
		if oldResult.err != nil {
			return nil, fmt.Errorf("error scanning new file: %v", oldResult.err)
		}

		// 合并结果，new的结果优先级高于sorted
		result := make(map[string]string)
		for k, v := range sortedResult.data {
			result[k] = v
		}
		for k, v := range oldResult.data {
			result[k] = v
		}
		return result, nil
	} else if kvs.anotherStartGC && !kvs.anotherEndGC {
		// GC中：并行查询上一轮新文件、上一轮排序文件和本轮new文件
		wg.Add(1) // 增加一个等待，因为要查询三个文件

		// 查询旧文件
		go func() {
			defer wg.Done()
			result := kvs.StartScan_opt(&kvrpc.ScanRangeRequest{
				StartKey: startKey,
				EndKey:   endKey,
			}, kvs.oldPersister, kvs.oldLog)
			oldChan <- scanResultOf(result)
		}()

		// 查询已排序文件
		go func() {
			defer wg.Done()
			result, err := kvs.scanFromSortedFile(startKey, endKey, kvs.lastSortedFileIndex)
			sortedChan <- scanResult{data: result, err: err}
		}()

		// 查询新文件
		go func() {
			defer wg.Done()
			result := kvs.StartScan_opt(&kvrpc.ScanRangeRequest{
				StartKey: startKey,
				EndKey:   endKey,
			}, kvs.persister, kvs.currentLog)
			newChan <- scanResultOf(result)
		}()

		wg.Wait()
		close(oldChan)
		close(sortedChan)
		close(newChan)

		oldResult := <-oldChan
		sortedResult := <-sortedChan
		newResult := <-newChan

		if oldResult.err != nil {
			return nil, fmt.Errorf("error scanning old file: %v", oldResult.err)
		}
		if sortedResult.err != nil {
			return nil, fmt.Errorf("error scanning sorted file: %v", sortedResult.err)
		}
		if newResult.err != nil {
			return nil, fmt.Errorf("error scanning new file: %v", newResult.err)
		}

		// 合并结果，优先级：new > old > sorted
		result := make(map[string]string)
		// 先加入sorted的结果
		for k, v := range sortedResult.data {
			result[k] = v
		}
		// 加入old的结果，覆盖sorted的
		for k, v := range oldResult.data {
			result[k] = v
		}
		// 最后加入new的结果，覆盖之前的
		for k, v := range newResult.data {
			result[k] = v
		}
		return result, nil

	} else {
		// GC后：并行查询本轮sorted和本轮new文件
		// 查询已排序文件
		go func() {
			defer wg.Done()
			result, err := kvs.scanFromSortedFile(startKey, endKey, kvs.anothersortedFileIndex)
			sortedChan <- scanResult{data: result, err: err}
		}()

		// 查询新文件
		go func() {
			defer wg.Done()
			result := kvs.StartScan_opt(&kvrpc.ScanRangeRequest{
				StartKey: startKey,
				EndKey:   endKey,
			}, kvs.persister, kvs.currentLog)
			newChan <- scanResultOf(result)
		}()

		wg.Wait()
		close(sortedChan)
		close(newChan)

		sortedResult := <-sortedChan
		newResult := <-newChan

		if sortedResult.err != nil {
			return nil, fmt.Errorf("error scanning sorted file: %v", sortedResult.err)
		}
		if newResult.err != nil {
			return nil, fmt.Errorf("error scanning new file: %v", newResult.err)
		}

		// 合并结果，new的结果优先级高于sorted
		result := make(map[string]string)
		for k, v := range sortedResult.data {
			result[k] = v
		}
		for k, v := range newResult.data {
			result[k] = v
		}
		return result, nil
	}
}

func (kvs *KVServer) firstGCScan(startKey, endKey string) (map[string]string, error) {
	var wg sync.WaitGroup
	wg.Add(2)

	sortedChan := make(chan scanResult, 1)
	newChan := make(chan scanResult, 1)

	if kvs.startGC && !kvs.endGC {
		// 并发查询旧文件
		go func() {
			defer wg.Done()
			result := kvs.StartScan_opt(&kvrpc.ScanRangeRequest{StartKey: startKey, EndKey: endKey}, kvs.oldPersister, kvs.oldLog)
			sortedChan <- scanResultOf(result)
		}()

		// 并发查询新文件
		go func() {
			defer wg.Done()
			result := kvs.StartScan_opt(&kvrpc.ScanRangeRequest{StartKey: startKey, EndKey: endKey}, kvs.persister, kvs.currentLog)
			// if err != nil {
			//     newChan <- scanResult{data: nil, err: err}
			//     return
			// }
			newChan <- scanResultOf(result)
		}()
	}
	if kvs.startGC && kvs.endGC {
		// 并发查询排序文件
		go func() {
			defer wg.Done()
			result, err := kvs.scanFromSortedFile(startKey, endKey, kvs.firstSortedFileIndex)
			sortedChan <- scanResult{data: result, err: err}
		}()

		// 并发查询新文件
		go func() {
			defer wg.Done()
			result := kvs.StartScan_opt(&kvrpc.ScanRangeRequest{StartKey: startKey, EndKey: endKey}, kvs.persister, kvs.currentLog)
			// if err != nil {
			//     newChan <- scanResult{data: nil, err: err}
			//     return
			// }
			newChan <- scanResultOf(result)
		}()
	}
	if !kvs.startGC {
		// 只查询旧文件
		go func() {
			defer wg.Done()
			result := kvs.StartScan_opt(&kvrpc.ScanRangeRequest{StartKey: startKey, EndKey: endKey}, kvs.persister, kvs.currentLog)
			sortedChan <- scanResultOf(result)
		}()
		wg.Done()
		wg.Wait()
		close(sortedChan)
		close(newChan)
		sortedResult := <-sortedChan
		if sortedResult.err != nil {
			return nil, fmt.Errorf("error scanning sorted file: %v", sortedResult.err)
		}
		result := make(map[string]string)
		for k, v := range sortedResult.data {
			result[k] = v
		}
		return result, nil //  不用合并，直接退出即可
	}
	// 等待两个查询都完成
	wg.Wait()
	close(sortedChan)
	close(newChan)

	// 获取结果
	sortedResult := <-sortedChan
	newResult := <-newChan

	// 检查错误
	if sortedResult.err != nil {
		return nil, fmt.Errorf("error scanning sorted file: %v", sortedResult.err)
	}
	if newResult.err != nil {
		return nil, fmt.Errorf("error scanning new file: %v", newResult.err)
	}

	// 合并结果
	result := make(map[string]string)
	for k, v := range newResult.data {
		result[k] = v
	}
	for k, v := range sortedResult.data {
		if _, exists := result[k]; !exists {
			result[k] = v
		}
	}

	return result, nil
}

func (kvs *KVServer) scanNewFile(startKey, endKey string, persister *raft.Persister, logLocation string) (map[string]string, error) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	result := make(map[string]string)
	paddedStartKey := kvs.persister.PadKey(startKey)
	paddedEndKey := kvs.persister.PadKey(endKey)

	// 从RocksDB中获取范围内的key-value对
	rdb := persister.GetDb()
	iter := rdb.NewIterator(ro)
	defer iter.Close()

	for iter.Seek([]byte(paddedStartKey)); iter.Valid(); iter.Next() {
		key := string(iter.Key().Data())
		if key > paddedEndKey {
			break
		}

		// 存储引擎里存的是什么，取决于当前配置——不能一律当成偏移解析。
		// 三种形态由首字节的标记区分，baseline 则根本没有标记。
		value, err := kvs.decodeScanValue(iter.Value().Data(), logLocation)
		if err != nil {
			return nil, err
		}
		originalKey := kvs.persister.UnpadKey(string(key))
		result[originalKey] = value
	}

	return result, nil
}

// decodeScanValue 把存储引擎里的一条记录还原成 value。
//
// SCAN 迭代拿到的字节串有三种可能，此前这里无条件当作偏移解析，于是另外两种
// 都会读出垃圾——baseline 对照组和 AVP placement 的 SCAN 因此都跑不出正确结果。
//
//	baseline (-kvSeparation=false)  裸 value，没有标记字节
//	[TagOffset, offset8]            KV 分离，去 valuelog 取
//	[TagInline, value...]           AVP 小值内联，就地取出
//
// 注意 TagOffset 记录共 9 字节，偏移在 [1:]。原先按 [0:8] 解析，把标记字节
// 当成了偏移的最低位——算出来的是"真实偏移左移 8 位再截断"，看似合法却指向
// 文件里的任意位置。
func (kvs *KVServer) decodeScanValue(raw []byte, logLocation string) (string, error) {
	if !kvs.kvSeparation {
		return string(raw), nil
	}
	if len(raw) == 0 {
		return "", errors.New("empty record in scan")
	}
	if raw[0] == raft.TagInline {
		return string(raw[1:]), nil
	}
	off, err := raft.DecodeOffsetRecord(raw)
	if err != nil {
		return "", err
	}
	return ReadValueFromOffset(off, logLocation)
}

// ==================================================
// ReadValueFromOffset 按偏移读出 value。
// 接口收的是解码好的 int64 而不是原始字节，这样"忘记剥标记字节"这类错误
// 没法再从调用点溜进来——解码只有 raft.DecodeOffsetRecord 一个入口。
func ReadValueFromOffset(position int64, logLocation string) (string, error) {

	// Open the file
	file, err := os.Open(logLocation)
	if err != nil {
		return "", fmt.Errorf("failed to open log file: %v", err)
	}
	defer file.Close()

	// Seek to the position
	_, err = file.Seek(position, 0)
	if err != nil {
		return "", fmt.Errorf("failed to seek in file: %v", err)
	}

	reader := bufio.NewReader(file)
	entry, _, err := ReadEntry(reader, 0) // 保留了 0，但你可能需要根据 ReadEntry 函数的实际需求调整这个值
	if err != nil {
		return "", fmt.Errorf("failed to read entry: %v", err)
	}

	return entry.Value, nil
}

func ReadEntry(reader *bufio.Reader, currentOffset int64) (*raft.Entry, int64, error) {
	var entry raft.Entry
	var keySize, valueSize uint32

	// Read all 20 bytes at once
	header := make([]byte, 20)
	n, err := io.ReadFull(reader, header)
	if err != nil {
		if err == io.EOF && n == 0 {
			return nil, 0, io.EOF // File is empty or we're at the end
		}
		return nil, 0, fmt.Errorf("failed to read header: %v (read %d bytes)", err, n)
	}

	// Parse the header
	keySize = binary.LittleEndian.Uint32(header[12:16])
	valueSize = binary.LittleEndian.Uint32(header[16:20])

	// Calculate total size
	entrySize := int64(20 + keySize + valueSize)

	// Read key and value
	data := make([]byte, keySize+valueSize)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read key and value: %v", err)
	}

	entry.Key = string(data[:keySize])
	entry.Value = string(data[keySize:])

	return &entry, entrySize, nil
}

// ==================================================

func (kvs *KVServer) firstGCGet(key string, reply *kvrpc.GetInRaftResponse) *kvrpc.GetInRaftResponse {
	if !kvs.startGC { // 还未开始GC，先去旧的rocksdb查询
		positionBytes, err := kvs.persister.Get_opt(key)
		if err != nil {
			fmt.Println("去旧的rocksdb中拿取key对应的index有问题")
			panic(err)
		}
		if positionBytes == -1 {
			reply.Err = raft.ErrNoKey
			reply.Value = raft.NoKey
			return reply
		}
		read_key, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
		if err != nil {
			fmt.Println("拿取value有问题")
			panic(err)
		}
		if read_key == kvs.persister.PadKey(key) {
			reply.Value = value
		} else {
			panic("错乱了，新的rocksdb中的key与index不匹配！！！")
		}
		return reply
	}

	type searchResult struct {
		found bool
		value string
		err   error
	}

	if kvs.startGC && !kvs.endGC {
		// 创建用于接收结果的通道
		newFileResult := make(chan searchResult, 1)
		oldFileResult := make(chan searchResult, 1)

		// 并行搜索新文件
		go func() {
			positionBytes, err := kvs.persister.Get_opt(key)
			if err != nil {
				newFileResult <- searchResult{false, "", err}
				return
			}
			if positionBytes == -1 {
				newFileResult <- searchResult{false, "", nil}
				return
			}
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
			if err != nil {
				newFileResult <- searchResult{false, "", err}
				return
			}
			if read_key == kvs.persister.PadKey(key) {
				newFileResult <- searchResult{true, value, nil}
			} else {
				newFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in new file")}
			}
		}()

		// 并行搜索旧文件
		go func() {
			positionBytes, err := kvs.oldPersister.Get_opt(key)
			if err != nil {
				oldFileResult <- searchResult{false, "", err}
				return
			}
			if positionBytes == -1 {
				oldFileResult <- searchResult{false, "", nil}
				return
			}
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.oldLog, positionBytes)
			if err != nil {
				oldFileResult <- searchResult{false, "", err}
				return
			}
			if read_key == kvs.persister.PadKey(key) {
				oldFileResult <- searchResult{true, value, nil}
			} else {
				oldFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in old file")}
			}
		}()

		// 首先检查新文件的结果
		select {
		case result := <-newFileResult:
			if result.err != nil {
				panic("去新的rocksdb中拿取key对应的index有问题")
			}
			if result.found {
				reply.Value = result.value
				return reply
			}
			// 如果新文件没找到，等待旧文件的结果
			result = <-oldFileResult
			if result.err != nil {
				panic("去旧的rocksdb中拿取key对应的index有问题")
			}
			if result.found {
				reply.Value = result.value
				return reply
			}
			reply.Err = raft.ErrNoKey
			reply.Value = raft.NoKey
			return reply
		}
	}

	if kvs.startGC && kvs.endGC {
		// 创建用于接收结果的通道
		newFileResult := make(chan searchResult, 1)
		sortedFileResult := make(chan searchResult, 1)

		// 并行搜索新文件
		go func() {
			positionBytes, err := kvs.persister.Get_opt(key)
			if err != nil {
				newFileResult <- searchResult{false, "", err}
				return
			}
			if positionBytes == -1 {
				newFileResult <- searchResult{false, "", nil}
				return
			}
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
			if err != nil {
				newFileResult <- searchResult{false, "", err}
				return
			}
			if read_key == kvs.persister.PadKey(key) {
				newFileResult <- searchResult{true, value, nil}
			} else {
				newFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in new file")}
			}
		}()

		// 并行搜索排序文件
		go func() {
			value, err := kvs.getFromSortedFile(key, kvs.firstSortedFileIndex)
			if err != nil {
				sortedFileResult <- searchResult{false, "", err}
				return
			}
			sortedFileResult <- searchResult{true, value, nil}
		}()

		// 首先检查新文件的结果
		select {
		case result := <-newFileResult:
			if result.err != nil {
				panic("去新的rocksdb中拿取key对应的index有问题")
			}
			if result.found {
				reply.Value = result.value
				return reply
			}
			// 如果新文件没找到，等待排序文件的结果
			result = <-sortedFileResult
			if result.err == nil {
				reply.Value = result.value
			} else {
				reply.Err = raft.ErrNoKey
				reply.Value = raft.NoKey
			}
			return reply
		}
	}
	return reply
}

func (kvs *KVServer) anotherGCGet(key string, reply *kvrpc.GetInRaftResponse) *kvrpc.GetInRaftResponse {
	// before-GC
	type searchResult struct {
		found bool
		value string
		err   error
	}
	if !kvs.anotherStartGC {
		// 创建用于接收结果的通道
		oldFileResult := make(chan searchResult, 1)
		lastSortedFileResult := make(chan searchResult, 1)

		// 并行搜索旧文件（上一轮的新文件），这时候还没开始第二轮GC，文件还没切换
		go func() {
			positionBytes, err := kvs.persister.Get_opt(key)
			if err != nil {
				oldFileResult <- searchResult{false, "", err}
				return
			}
			if positionBytes == -1 {
				oldFileResult <- searchResult{false, "", nil}
				return
			}
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
			if err != nil {
				oldFileResult <- searchResult{false, "", err}
				return
			}
			if read_key == kvs.persister.PadKey(key) {
				oldFileResult <- searchResult{true, value, nil}
			} else {
				oldFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in new file")}
			}
		}()

		// 并行搜索排序文件，这个排序文件在第一轮GC完就已经切换，所以下面的不用改
		go func() {
			value, err := kvs.getFromSortedFile(key, kvs.lastSortedFileIndex)
			if err != nil {
				lastSortedFileResult <- searchResult{false, "", err}
				return
			}
			lastSortedFileResult <- searchResult{true, value, nil}
		}()

		// 首先检查新文件的结果
		select {
		case result := <-oldFileResult:
			if result.err != nil {
				panic("去新的rocksdb中拿取key对应的index有问题")
			}
			if result.found {
				reply.Value = result.value
				return reply
			}
			// 如果新文件没找到，等待排序文件的结果
			result = <-lastSortedFileResult
			if result.err == nil {
				reply.Value = result.value
			} else {
				reply.Err = raft.ErrNoKey
				reply.Value = raft.NoKey
			}
			return reply
		}
	}
	// during-GC
	if !kvs.anotherEndGC {
		// 创建用于接收结果的通道
		newFileResult := make(chan searchResult, 1)
		oldFileResult := make(chan searchResult, 1)
		lastSortedFileResult := make(chan searchResult, 1)

		// 并行搜索旧文件（上一轮的新文件）
		go func() {
			positionBytes, err := kvs.oldPersister.Get_opt(key)
			if err != nil {
				oldFileResult <- searchResult{false, "", err}
				return
			}
			if positionBytes == -1 {
				oldFileResult <- searchResult{false, "", nil}
				return
			}
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.oldLog, positionBytes)
			if err != nil {
				oldFileResult <- searchResult{false, "", err}
				return
			}
			if read_key == kvs.oldPersister.PadKey(key) {
				oldFileResult <- searchResult{true, value, nil}
			} else {
				oldFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in new file")}
			}
		}()

		// 并行搜索新文件（本轮的新文件）
		go func() {
			positionBytes, err := kvs.persister.Get_opt(key)
			if err != nil {
				newFileResult <- searchResult{false, "", err}
				return
			}
			if positionBytes == -1 {
				newFileResult <- searchResult{false, "", nil}
				return
			}
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
			if err != nil {
				newFileResult <- searchResult{false, "", err}
				return
			}
			if read_key == kvs.persister.PadKey(key) {
				newFileResult <- searchResult{true, value, nil}
			} else {
				newFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in new file")}
			}
		}()

		// 并行搜索排序文件
		go func() {
			value, err := kvs.getFromSortedFile(key, kvs.lastSortedFileIndex)
			if err != nil {
				lastSortedFileResult <- searchResult{false, "", err}
				return
			}
			lastSortedFileResult <- searchResult{true, value, nil}
		}()

		// 首先检查新文件的结果，再旧文件，再排序文件
		select {
		case result := <-newFileResult:
			if result.err != nil {
				panic(fmt.Sprintf("去新的rocksdb中拿取key对应的index有问题: %v", result.err))
			}
			if result.found {
				reply.Value = result.value
				return reply
			}
			result = <-oldFileResult
			if result.err != nil {
				panic(fmt.Sprintf("去旧的rocksdb中拿取key对应的index有问题: %v", result.err))
			}
			if result.found {
				reply.Value = result.value
				return reply
			}
			// 如果新文件没找到，等待排序文件的结果
			result = <-lastSortedFileResult
			if result.err == nil {
				reply.Value = result.value
			} else {
				reply.Err = raft.ErrNoKey
				reply.Value = raft.NoKey
			}
			return reply
		}
	}
	// post-GC
	// 创建用于接收结果的通道
	newFileResult := make(chan searchResult, 1)
	anotherSortedFileResult := make(chan searchResult, 1)

	// 并行搜索新文件（本轮的新文件）
	go func() {
		positionBytes, err := kvs.persister.Get_opt(key)
		if err != nil {
			newFileResult <- searchResult{false, "", err}
			return
		}
		if positionBytes == -1 {
			newFileResult <- searchResult{false, "", nil}
			return
		}
		read_key, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
		if err != nil {
			newFileResult <- searchResult{false, "", err}
			return
		}
		if read_key == kvs.persister.PadKey(key) {
			newFileResult <- searchResult{true, value, nil}
		} else {
			newFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in new file")}
		}
	}()

	// 并行搜索排序文件
	go func() {
		value, err := kvs.getFromSortedFile(key, kvs.anothersortedFileIndex)
		if err != nil {
			anotherSortedFileResult <- searchResult{false, "", err}
			return
		}
		anotherSortedFileResult <- searchResult{true, value, nil}
	}()

	// 首先检查新文件的结果
	select {
	case result := <-newFileResult:
		if result.err != nil {
			panic("去新的rocksdb中拿取key对应的index有问题")
		}
		if result.found {
			reply.Value = result.value
			return reply
		}
		// 如果新文件没找到，等待排序文件的结果
		result = <-anotherSortedFileResult
		if result.err == nil {
			reply.Value = result.value
		} else {
			reply.Err = raft.ErrNoKey
			reply.Value = raft.NoKey
		}
		return reply
	}
}

// getFromSortedFile looks a key up in a sorted file: inline cache first, then the sparse index and a block scan.
func (kvs *KVServer) getFromSortedFile(key string, index *SortedFileIndex) (string, error) {
	// 先检查LRU缓存
	// if value, ok := kvs.sortedFileCache.Get(key); ok {
	// 	// 缓存命中，直接返回缓存的value
	// 	return value.(string), nil
	// }
	// 增加参数检查
	if index == nil {
		return "", errors.New("invalid index: index is nil")
	}

	// 先查内联缓存，命中则免去文件 I/O
	if value, ok := index.InlineValues.Get(key); ok {
		avpRecordHit()
		return string(value), nil
	}
	avpRecordMiss()

	// 未命中：经稀疏索引二分定位到块，块内顺序扫描
	entry, err := kvs.lookupInSortedFile(index, key)
	if err != nil {
		// 这里不能记 not_found。GC 之后数据分散在多个 sortedFile 与新旧 valuelog 中，
		// 一次读会并发查这几处，"这个分片里没有"是常态而非键缺失——照此计数会把
		// 分片未命中当成键不存在（实测虚高到 37%）。真正的判定在 GetInRaft，
		// 那里是所有查找路径唯一的汇合点。
		return "", err
	}

	// 小值回填内联缓存，供后续读命中（Zipf 热点下命中率很高）
	if len(entry.Value) < kvs.inlineThreshold {
		index.InlineValues.Add(key, entry.Value)
	}

	return entry.Value, nil
}

// ReadEntryFromMMap 从内存映射中读取条目
func ReadEntryFromMMap(data []byte) (*raft.Entry, int, error) {
	var entry raft.Entry
	var entrySize int

	// 读取固定长度的字段
	if len(data) < 20 {
		return nil, 0, errors.New("insufficient data")
	}

	entry.Index = binary.LittleEndian.Uint32(data[0:4])
	entry.CurrentTerm = binary.LittleEndian.Uint32(data[4:8])
	entry.VotedFor = binary.LittleEndian.Uint32(data[8:12])
	keySize := binary.LittleEndian.Uint32(data[12:16])
	valueSize := binary.LittleEndian.Uint32(data[16:20])

	entrySize = 20 + int(keySize) + int(valueSize)

	if len(data) < entrySize {
		return nil, 0, errors.New("insufficient data")
	}

	entry.Key = string(data[20 : 20+keySize])
	entry.Value = string(data[20+keySize : entrySize])

	return &entry, entrySize, nil
}

// scanFromSortedFile returns every key in [startKey, endKey] from a sorted file, using the
// sparse index to bound the byte range and a memory map to walk it.
func (kvs *KVServer) scanFromSortedFile(startKey, endKey string, index *SortedFileIndex) (map[string]string, error) {

	paddedStartKey := kvs.persister.PadKey(startKey)
	paddedEndKey := kvs.persister.PadKey(endKey)

	result := make(map[string]string)

	// 范围查询直接走 sortedFile 顺序读：Entries 已覆盖所有 key（含小值），
	// 且顺序读本就是范围查询的最优路径。不再遍历内联缓存——那是 O(缓存条目数)，
	// 与查询范围无关，小值场景下会让窄范围 scan 退化。
	// 用稀疏索引二分定位扫描起点。原先是从 startKey 起逐个 +1 试探直到命中，
	// 复杂度随键空间稀疏程度恶化；二分与之无关。
	startOffset, ok := index.firstBlockAtOrAfter(paddedStartKey)
	if !ok { // 索引为空，文件里没有数据
		return nil, nil
	}

	// 找到大于等于 startKey 的最小索引项
	// startOffset, exists := index.GetOffset(startKey)
	// if !exists {
	//     // 如果精确的startKey不存在，找到下一个最近的键
	//     for key, offset := range index.Entries {
	//         if kvs.persister.PadKey(key) >= paddedStartKey {
	//             startOffset = offset
	//             break
	//         }
	//     }
	// }

	// 打开文件
	// file, err := os.Open(index.FilePath)
	// if err != nil {
	// 	return nil, err
	// }
	// defer file.Close()
	// 由直接打开文件替换为从池中获取文件描述符
	file, err := kvs.filePool.Get()
	if err != nil {
		return nil, errors.New("获取文件描述符失败！")
	}
	defer kvs.filePool.Put(file) // 使用完毕后归还到池中

	// 获取文件信息
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()

	// 创建内存映射
	mmap, err := mmap.Map(file, mmap.RDONLY, 0)

	if err != nil {
		return nil, err
	}
	defer mmap.Unmap()

	// 从startOffset开始读取和处理数据
	for offset := startOffset; offset < fileSize; {
		entry, entrySize, err := ReadEntryFromMMap(mmap[offset:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if entry.Key > paddedEndKey {
			break // 已经超过了endKey，结束扫描
		}

		if entry.Key >= paddedStartKey {
			unpadKey := kvs.persister.UnpadKey(entry.Key)
			result[unpadKey] = entry.Value
		}

		offset += int64(entrySize)
	}

	return result, nil
}
