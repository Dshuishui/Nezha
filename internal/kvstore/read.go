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
	// 读锁要盖住"读出下面那几个字段"到"迭代结束"的整段，理由见 KVServer.storeRetireMu。
	// 子 goroutine 不再各取一次：读锁由父 goroutine 持有、wg.Wait 之前不释放，
	// 子 goroutine 都在这个窗口内跑完。
	kvs.storeRetireMu.RLock()
	defer kvs.storeRetireMu.RUnlock()
	var wg sync.WaitGroup
	wg.Add(2)

	oldChan := make(chan scanResult, 1)
	sortedChan := make(chan scanResult, 1)
	newChan := make(chan scanResult, 1)

	if !kvs.anotherStartGC {
		// GC前：并行查询上一轮新文件，上一轮排序文件
		go func() {
			defer wg.Done()
			result, err := kvs.scanFromPartitions(startKey, endKey, kvs.lastPartitions)
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
			result, err := kvs.scanFromPartitions(startKey, endKey, kvs.lastPartitions)
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
			result, err := kvs.scanFromPartitions(startKey, endKey, kvs.anotherPartitions)
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
	// 同 anotherGCScan，理由见 KVServer.storeRetireMu。
	kvs.storeRetireMu.RLock()
	defer kvs.storeRetireMu.RUnlock()
	var wg sync.WaitGroup
	wg.Add(2)

	sortedChan := make(chan scanResult, 1)
	newChan := make(chan scanResult, 1)

	if kvs.startGC {
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

// scanNewFile 在存储引擎上做一次范围迭代，逐条还原 value。
//
// **这里不取任何锁。** 曾经整段迭代都持 kvs.mu，而 applyLoop 用的是同一把锁，
// 于是一次扫描把写入路径按住整段扫描时长（实测数字见 KVServer.storeRetireMu）。
// 那把锁真正在做的事只有一件：让 GC 关掉被取代的存储引擎排到扫描之后。现在这件事
// 由 storeRetireMu 单独做——扫描入口持它的读锁、removeSupersededStore 持写锁，
// 而 apply 一概不碰它。
//
// 本函数自己不需要任何互斥：persister 与 logLocation 都是参数传进来的，
// kvs.decodeScanValue 只读 kvs.kvSeparation（启动后不变的配置）。
//
// 整段扫描只开一次 valuelog（见下面的 logReader），而不是每个 key 开关一次文件。
func (kvs *KVServer) scanNewFile(startKey, endKey string, persister *raft.Persister, logLocation string) (map[string]string, error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	result := make(map[string]string)

	// 整段扫描只开一次 valuelog。原先是 decodeScanValue → ReadValueFromOffset，
	// 里面每个 key 都 os.Open + bufio.NewReader + Close 一遍：一次 1000 条的扫描就是
	// 1000 次开关文件加 1000 个 4KB 缓冲区，而这一切还发生在 kvs.mu 之内。
	// 有序文件那条路早就用描述符池 + mmap 避开了这件事，valuelog 这条路没有。
	// 惰性打开：baseline（无标记裸 value）与全内联的情形一次都不需要碰这个文件。
	lr := &logReader{path: logLocation}
	defer lr.Close()

	// 从RocksDB中获取范围内的key-value对
	rdb := persister.GetDb()
	iter := rdb.NewIterator(ro)
	defer iter.Close()

	for iter.Seek([]byte(startKey)); iter.Valid(); iter.Next() {
		key := string(iter.Key().Data())
		if key > endKey {
			break
		}
		// 恢复用的 applied index 与用户数据同库，首字节 0x00，排在所有可打印 key 之前。
		// 补齐是存储层做的那会儿，Seek 到一个补过零的 startKey 天然跳过它；现在 key 原样
		// 存，一次 startKey 很小（或 KeyPadNone 下的 YCSB key）的扫描会 Seek 到库首，
		// 把那一行喂给 decodeScanValue——8 字节的 applied index 会被当成一条用户记录。
		if raft.IsMetaKey(iter.Key().Data()) {
			continue
		}

		// 存储引擎里存的是什么，取决于当前配置——不能一律当成偏移解析。
		// 三种形态由首字节的标记区分，baseline 则根本没有标记。
		value, err := kvs.decodeScanValue(iter.Value().Data(), lr)
		if err != nil {
			return nil, err
		}
		originalKey := string(key)
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
func (kvs *KVServer) decodeScanValue(raw []byte, lr *logReader) (string, error) {
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
	return lr.valueAt(off)
}

// ==================================================
// logReader 是一次扫描期间复用的 valuelog 句柄：句柄和 bufio 缓冲区各只建一次，
// 每个 key 只付一次 Seek。惰性打开，所以不需要读 valuelog 的扫描一次 open 都不做。
// 不并发使用——一次扫描是单 goroutine 顺序迭代的。
type logReader struct {
	path string
	f    *os.File
	br   *bufio.Reader
}

func (lr *logReader) valueAt(position int64) (string, error) {
	if lr.f == nil {
		f, err := os.Open(lr.path)
		if err != nil {
			return "", fmt.Errorf("failed to open log file: %v", err)
		}
		lr.f, lr.br = f, bufio.NewReader(f)
	}
	if _, err := lr.f.Seek(position, io.SeekStart); err != nil {
		return "", fmt.Errorf("failed to seek in file: %v", err)
	}
	// Seek 之后缓冲区里还压着上一个位置读出来的字节，必须 Reset，否则读到的是旧内容。
	lr.br.Reset(lr.f)
	entry, _, err := ReadEntry(lr.br, 0)
	if err != nil {
		return "", fmt.Errorf("failed to read entry: %v", err)
	}
	return entry.Value, nil
}

func (lr *logReader) Close() {
	if lr.f != nil {
		lr.f.Close()
		lr.f, lr.br = nil, nil
	}
}

// ReadValueFromOffset 按偏移读出 value（单次读用；扫描请用 logReader，见上）。
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
	if !kvs.startGC { // 还未开始 GC，只有一路可查：当前 rocksdb 的偏移 + 当前 valuelog
		var out readOutcome
		positionBytes, err := kvs.persister.Get_opt(key)
		if err != nil {
			out.note("当前 rocksdb 取偏移", err)
			return out.finish(reply, key)
		}
		if positionBytes == -1 {
			return out.finish(reply, key) // 没有记录：键不存在
		}
		readKey, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
		if err != nil {
			out.note("当前 valuelog 按偏移读", err)
			return out.finish(reply, key)
		}
		if readKey != key {
			// 偏移指向了别的记录：索引与日志不配套，是个真问题。但它在**读侧**，
			// 报上去并带上现场，不要带走整个节点。
			out.note("当前 valuelog", fmt.Errorf("偏移 %d 处的 key 是 %q，与请求的 %q 不符",
				positionBytes, readKey, key))
			return out.finish(reply, key)
		}
		reply.Value = value
		return reply
	}

	type searchResult struct {
		found bool
		value string
		err   error
		// cacheHit 只对分区那一路有意义：这次查找是不是被内联缓存接住了。
		// 计数留到汇合点，因为"这一路的结果有没有被采用"只有那里知道。
		cacheHit bool
	}

	if kvs.startGC {
		// 创建用于接收结果的通道
		newFileResult := make(chan searchResult, 1)
		oldFileResult := make(chan searchResult, 1)

		// 并行搜索新文件
		go func() {
			positionBytes, err := kvs.persister.Get_opt(key)
			if err != nil {
				newFileResult <- searchResult{false, "", err, false}
				return
			}
			if positionBytes == -1 {
				newFileResult <- searchResult{false, "", nil, false}
				return
			}
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
			if err != nil {
				newFileResult <- searchResult{false, "", err, false}
				return
			}
			if read_key == key {
				newFileResult <- searchResult{true, value, nil, false}
			} else {
				newFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in new file"), false}
			}
		}()

		// 并行搜索旧文件
		go func() {
			positionBytes, err := kvs.oldPersister.Get_opt(key)
			if err != nil {
				oldFileResult <- searchResult{false, "", err, false}
				return
			}
			if positionBytes == -1 {
				oldFileResult <- searchResult{false, "", nil, false}
				return
			}
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.oldLog, positionBytes)
			if err != nil {
				oldFileResult <- searchResult{false, "", err, false}
				return
			}
			if read_key == key {
				oldFileResult <- searchResult{true, value, nil, false}
			} else {
				oldFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in old file"), false}
			}
		}()

		// 按优先级逐路取结果：新文件在前，同一个 key 的较新写入在那里。
		var out readOutcome
		result := <-newFileResult
		out.note("新 valuelog", result.err)
		if result.found {
			reply.Value = result.value
			return reply
		}
		result = <-oldFileResult
		out.note("旧 valuelog", result.err)
		if result.found {
			reply.Value = result.value
			return reply
		}
		return out.finish(reply, key)
	}

	return reply
}

func (kvs *KVServer) anotherGCGet(key string, reply *kvrpc.GetInRaftResponse) *kvrpc.GetInRaftResponse {
	// before-GC
	type searchResult struct {
		found bool
		value string
		err   error
		// cacheHit 只对分区那一路有意义：这次查找是不是被内联缓存接住了。
		// 计数留到汇合点，因为"这一路的结果有没有被采用"只有那里知道。
		cacheHit bool
	}
	if !kvs.anotherStartGC {
		// 创建用于接收结果的通道
		oldFileResult := make(chan searchResult, 1)
		lastSortedFileResult := make(chan searchResult, 1)

		// 并行搜索旧文件（上一轮的新文件），这时候还没开始第二轮GC，文件还没切换
		go func() {
			positionBytes, err := kvs.persister.Get_opt(key)
			if err != nil {
				oldFileResult <- searchResult{false, "", err, false}
				return
			}
			if positionBytes == -1 {
				oldFileResult <- searchResult{false, "", nil, false}
				return
			}
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
			if err != nil {
				oldFileResult <- searchResult{false, "", err, false}
				return
			}
			if read_key == key {
				oldFileResult <- searchResult{true, value, nil, false}
			} else {
				oldFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in new file"), false}
			}
		}()

		// 并行搜索排序文件，这个排序文件在第一轮GC完就已经切换，所以下面的不用改
		go func() {
			value, hit, err := kvs.getFromPartitions(key, kvs.lastPartitions)
			if err != nil {
				lastSortedFileResult <- searchResult{false, "", err, false}
				return
			}
			lastSortedFileResult <- searchResult{true, value, nil, hit}
		}()

		// 按优先级逐路取结果：当前 valuelog 在前
		var out readOutcome
		result := <-oldFileResult
		out.note("当前 valuelog", result.err)
		if result.found {
			// 这次读是 valuelog 答的，内联缓存本来就服务不了它（缓存只装已搬进分区的小值）。
			// 计入 served_by_log，**不**计入 hit/miss，见 avpstats.go。
			avpRecordServedByLog()
			reply.Value = result.value
			return reply
		}
		result = <-lastSortedFileResult
		if result.err == nil {
			avpRecordPartitionRead(result.cacheHit)
			reply.Value = result.value
			return reply
		}
		out.note("上一轮分区", result.err)
		return out.finish(reply, key)
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
				oldFileResult <- searchResult{false, "", err, false}
				return
			}
			if positionBytes == -1 {
				oldFileResult <- searchResult{false, "", nil, false}
				return
			}
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.oldLog, positionBytes)
			if err != nil {
				oldFileResult <- searchResult{false, "", err, false}
				return
			}
			if read_key == key {
				oldFileResult <- searchResult{true, value, nil, false}
			} else {
				oldFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in new file"), false}
			}
		}()

		// 并行搜索新文件（本轮的新文件）
		go func() {
			positionBytes, err := kvs.persister.Get_opt(key)
			if err != nil {
				newFileResult <- searchResult{false, "", err, false}
				return
			}
			if positionBytes == -1 {
				newFileResult <- searchResult{false, "", nil, false}
				return
			}
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
			if err != nil {
				newFileResult <- searchResult{false, "", err, false}
				return
			}
			if read_key == key {
				newFileResult <- searchResult{true, value, nil, false}
			} else {
				newFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in new file"), false}
			}
		}()

		// 并行搜索排序文件
		go func() {
			value, hit, err := kvs.getFromPartitions(key, kvs.lastPartitions)
			if err != nil {
				lastSortedFileResult <- searchResult{false, "", err, false}
				return
			}
			lastSortedFileResult <- searchResult{true, value, nil, hit}
		}()

		// 按优先级逐路取结果：新文件、旧文件、上一轮分区
		var out readOutcome
		result := <-newFileResult
		out.note("新 valuelog", result.err)
		if result.found {
			avpRecordServedByLog()
			reply.Value = result.value
			return reply
		}
		result = <-oldFileResult
		out.note("旧 valuelog", result.err)
		if result.found {
			avpRecordServedByLog()
			reply.Value = result.value
			return reply
		}
		result = <-lastSortedFileResult
		if result.err == nil {
			avpRecordPartitionRead(result.cacheHit)
			reply.Value = result.value
			return reply
		}
		out.note("上一轮分区", result.err)
		return out.finish(reply, key)
	}
	// post-GC
	// 创建用于接收结果的通道
	newFileResult := make(chan searchResult, 1)
	anotherSortedFileResult := make(chan searchResult, 1)

	// 并行搜索新文件（本轮的新文件）
	go func() {
		positionBytes, err := kvs.persister.Get_opt(key)
		if err != nil {
			newFileResult <- searchResult{false, "", err, false}
			return
		}
		if positionBytes == -1 {
			newFileResult <- searchResult{false, "", nil, false}
			return
		}
		read_key, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
		if err != nil {
			newFileResult <- searchResult{false, "", err, false}
			return
		}
		if read_key == key {
			newFileResult <- searchResult{true, value, nil, false}
		} else {
			newFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in new file"), false}
		}
	}()

	// 并行搜索排序文件
	go func() {
		value, hit, err := kvs.getFromPartitions(key, kvs.anotherPartitions)
		if err != nil {
			anotherSortedFileResult <- searchResult{false, "", err, false}
			return
		}
		anotherSortedFileResult <- searchResult{true, value, nil, hit}
	}()

	// 按优先级逐路取结果：当前 valuelog 在前，本轮分区在后
	var out readOutcome
	result := <-newFileResult
	out.note("当前 valuelog", result.err)
	if result.found {
		avpRecordServedByLog()
		reply.Value = result.value
		return reply
	}
	result = <-anotherSortedFileResult
	if result.err == nil {
		avpRecordPartitionRead(result.cacheHit)
		reply.Value = result.value
		return reply
	}
	out.note("本轮分区", result.err)
	return out.finish(reply, key)
}

// readOutcome 汇总一次**多路查找**的结果。
//
// GET 会并发查几处（当前 valuelog、上一轮 valuelog、分区文件），按优先级逐路取结果。
// 这里要守住两条规矩，此前只有第二条在有序文件那一路上成立：
//
//  1. **某一路出错不是答案。** 记下它、继续问下一路——第一路的一次读失败不能盖掉
//     第二路手里正确的 value。原先是 panic，所以连"继续问"都没有机会。
//  2. **全都没找到时，出过错就是 ErrInternal，没出错才是 ErrNoKey。** 把读失败
//     报成"键不存在"会让丢数据看起来像负载配置问题（2026-09-09 定的规矩）。
//     而 ErrKeyAbsent 是"这一处没有"，是常态，不算错误。
//
// 原先 valuelog/RocksDB 那几路是直接 panic（9 处）：一次读失败带走整个节点，
// 也就是带走一个 Raft 成员。读失败并不威胁 Raft 的安全性论证——那是
// persistHardState 里 panic 的理由，不是这里的。现场信息保留在日志里。
type readOutcome struct {
	err   error
	where string
}

// note 记下某一路的失败。只留第一个，后面的路只是补充不了新信息的同类错误。
func (o *readOutcome) note(where string, err error) {
	if err == nil || errors.Is(err, ErrKeyAbsent) {
		return // "这一处没有"是常态
	}
	if o.err == nil {
		o.err, o.where = err, where
	}
}

// finish 在所有查找路径都没找到 key 时给出最终应答。
//
// 出过错就必须是 ErrInternal：读失败时"这个 key 到底存不存在"是**未知**的，
// 不能替客户端断言它不存在。（这条判据原先在一个独立的 setReadFailure 里，
// readOutcome 接手之后它就没有调用点了——两份并存迟早漂移，所以并进来。）
func (o *readOutcome) finish(reply *kvrpc.GetInRaftResponse, key string) *kvrpc.GetInRaftResponse {
	if o.err != nil {
		fmt.Printf("[READ] key=%q 没有任何一路找到，且「%s」出过错，按 ErrInternal 上报（不是 NOKEY）: %v\n",
			key, o.where, o.err)
		reply.Err = raft.ErrInternal
		reply.Value = raft.NoKey
		return reply
	}
	reply.Err = raft.ErrNoKey
	reply.Value = raft.NoKey
	return reply
}

// ErrKeyAbsent 表示"这一处没有这个 key"，与"读取失败"必须区分开。
//
// GC 之后数据分散在若干分区与新旧 valuelog 中，一次读并发查这几处，"这一处没有"是常态；
// 而读取失败（文件打不开、I/O 出错、记录损坏）是异常。此前两者都只是 error，调用方一律
// 当作 NOKEY 回给客户端——**一次真正的读取失败会变成一个确定的"key 不存在"**，
// 客户端据此认定数据没了。用哨兵把两者分开，调用方才有得判。
var ErrKeyAbsent = errors.New(raft.ErrNoKey)

// getFromPartitions 把一次点查路由到唯一可能含有该 key 的分区。
//
// 分区区间互不重叠，候选因此只有一个，查找本身仍是原来的"稀疏索引二分 + 块内顺序扫描"。
// 相对改造前的单个大文件，索引更小、局部性更好。
//
// cacheHit 只是**回报**这次查找是不是被内联缓存接住的，计数留给汇合点：
// 这一路的结果可能压根没被采用（当前 valuelog 优先），没被采用的查找不该算进命中率。
func (kvs *KVServer) getFromPartitions(key string, ps *PartitionSet) (_ string, cacheHit bool, _ error) {
	if ps == nil {
		return "", false, errors.New("invalid partition set: set is nil")
	}
	part := ps.find(key)
	if part == nil {
		// key 比所有分区都小，或落在两个分区之间的空隙里——都等价于这组分区里没有它。
		// 内联缓存不必在这里查：缓存只由写进某个分区的 entry 填充，被缓存的 key 必然落在
		// 某个分区的区间内，走不到这个分支。
		// 也**不计 miss**：这一路没有答案，最终是不是"键不存在"由汇合点判（avpRecordNotFound）。
		return "", false, ErrKeyAbsent
	}
	return kvs.getFromSortedFile(key, part)
}

// scanFromPartitions 把一次范围查询路由到与 [startKey, endKey] 有交集的那些分区，
// 按 key 序依次扫描并拼接。
//
// 分区内部仍是一段连续读——范围查询的最优路径没有变，变的只是它可能横跨若干个文件。
// 窄范围通常只落在一个分区里，走单分区快路径；全范围扫描会触及全部分区，多出的代价是
// 每个分区一次打开与 mmap。
func (kvs *KVServer) scanFromPartitions(startKey, endKey string, ps *PartitionSet) (map[string]string, error) {
	if ps == nil {
		return nil, nil
	}
	parts := ps.overlapping(startKey, endKey)
	if len(parts) == 0 {
		return nil, nil
	}
	// 所有分区**直接写进同一个 map**，而不是各建一个再合并。分区区间互不重叠，
	// 同一个 key 不会出现在两个分区里，写进同一个 map 是安全的。
	//
	// 先前的写法每个分区各建一个 map、再逐条拷进结果，等于把每条记录插两遍。窄范围只碰
	// 一个分区时有快路径绕过，看不出来；跨分区时就实打实付这份代价——64B 档一次扫描
	// 35.7 万条，实测跨分区 SCAN 的 p50 因此比改造前高 11%，正是这一项。
	result := make(map[string]string)
	for _, part := range parts {
		if err := kvs.scanFromSortedFileInto(result, startKey, endKey, part); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// getFromSortedFile looks a key up in a sorted file: inline cache first, then the sparse index and a block scan.
func (kvs *KVServer) getFromSortedFile(key string, index *SortedFileIndex) (_ string, cacheHit bool, _ error) {
	// 先检查LRU缓存
	// if value, ok := kvs.sortedFileCache.Get(key); ok {
	// 	// 缓存命中，直接返回缓存的value
	// 	return value.(string), nil
	// }
	// 增加参数检查
	if index == nil {
		return "", false, errors.New("invalid index: index is nil")
	}

	// 先查内联缓存，命中则免去文件 I/O。
	// **命中与否不在这里计数**：这一次查找的结果可能压根没被采用（GET 是多路并发查找，
	// 当前 valuelog 优先），而"没被采用的那次查找"不该算进内联缓存的命中率。
	// 计数放在汇合点（anotherGCGet 的三个分支），由那里决定是哪条路答的。
	if value, ok := index.InlineValues.Get(key); ok {
		return string(value), true, nil
	}

	// 未命中：经稀疏索引二分定位到块，块内顺序扫描
	entry, err := kvs.lookupInSortedFile(index, key)
	if err != nil {
		// 这里不能记 not_found。GC 之后数据分散在多个 sortedFile 与新旧 valuelog 中，
		// 一次读会并发查这几处，"这个分片里没有"是常态而非键缺失——照此计数会把
		// 分片未命中当成键不存在（实测虚高到 37%）。真正的判定在 GetInRaft，
		// 那里是所有查找路径唯一的汇合点。
		return "", false, err
	}

	// 小值回填内联缓存，供后续读命中（Zipf 热点下命中率很高）
	if kvs.shouldInline(len(entry.Value)) {
		index.InlineValues.Add(key, entry.Value)
	}

	return entry.Value, false, nil
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

// scanFromSortedFileInto 把命中的键值写进调用方给的 map，供跨分区扫描共用一个结果集。
func (kvs *KVServer) scanFromSortedFileInto(result map[string]string, startKey, endKey string, index *SortedFileIndex) error {

	// 范围查询直接走 sortedFile 顺序读：Entries 已覆盖所有 key（含小值），
	// 且顺序读本就是范围查询的最优路径。不再遍历内联缓存——那是 O(缓存条目数)，
	// 与查询范围无关，小值场景下会让窄范围 scan 退化。
	// 用稀疏索引二分定位扫描起点。原先是从 startKey 起逐个 +1 试探直到命中，
	// 复杂度随键空间稀疏程度恶化；二分与之无关。
	startOffset, ok := index.firstBlockAtOrAfter(startKey)
	if !ok { // 索引为空，文件里没有数据
		return nil
	}

	// 找到大于等于 startKey 的最小索引项
	// startOffset, exists := index.GetOffset(startKey)
	// if !exists {
	//     // 如果精确的startKey不存在，找到下一个最近的键
	//     for key, offset := range index.Entries {
	//         if key >= startKey {
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
	// 由直接打开文件替换为从池中获取文件描述符。池跟着 index 走而不是挂在 KVServer 上：
	// 分区化之后一个进程同时持有多个有序文件，全局单例会读错文件。
	if index.pool == nil {
		return fmt.Errorf("sorted file %s has no descriptor pool", index.FilePath)
	}
	file, err := index.pool.Get()
	if err != nil {
		return fmt.Errorf("获取文件描述符失败（%s）: %v", index.FilePath, err)
	}
	defer index.pool.Put(file) // 使用完毕后归还到池中

	// 获取文件信息
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	// 创建内存映射
	mmap, err := mmap.Map(file, mmap.RDONLY, 0)

	if err != nil {
		return err
	}
	defer mmap.Unmap()

	// 从startOffset开始读取和处理数据
	for offset := startOffset; offset < fileSize; {
		entry, entrySize, err := ReadEntryFromMMap(mmap[offset:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if entry.Key > endKey {
			break // 已经超过了endKey，结束扫描
		}

		if entry.Key >= startKey {
			unpadKey := entry.Key
			result[unpadKey] = entry.Value
		}

		offset += int64(entrySize)
	}

	return nil
}
