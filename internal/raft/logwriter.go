package raft

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
)

// The value log doubles as the Raft log: every entry is appended here once, and the
// offsets recorded at write time are what the KV layer stores in RocksDB.

func (rf *Raft) GetOffsets() []int64 {
	return rf.Offsets
}

// OldestPendingVersion returns the file version of the oldest entry that is written to the
// log but not yet applied, or false when nothing is pending. GC uses it to decide whether
// everything in the old file has reached the old index: Offsets/offsetVersions is a queue
// consumed in apply order, so its head is the oldest unapplied entry.
func (rf *Raft) OldestPendingVersion() (int32, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.offsetVersions) == 0 {
		return 0, false
	}
	return rf.offsetVersions[0], true
}

// openLogFile 打开（或重开）日志文件并接管写入位置。调用方必须持有 rf.logMu。
func (rf *Raft) openLogFile(filename string) error {
	if rf.logWriter != nil {
		rf.logWriter.Flush()
	}
	if rf.logFile != nil {
		rf.logFile.Close()
	}
	// 不能用 O_APPEND：POSIX 规定该模式下每次写入前偏移量强制设为文件末尾，
	// Seek 对写入位置完全无效。冲突覆盖（startPos != 0）依赖 Seek 回退，在
	// O_APPEND 下会静默变成追加——文件末尾多出一条记录，而 rf.Offsets 里记的
	// 是 startPos，照这个偏移读出来的是本该被覆盖掉的旧数据，且不报任何错。
	// 写入位置由 rf.logOffset 自行维护，本就不需要 O_APPEND。
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	end, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return err
	}
	rf.logFile = f
	rf.logWriter = bufio.NewWriterSize(f, 1<<20)
	rf.logOffset = end
	return nil
}

// CloseLogFile 刷净缓冲并释放句柄。
// EnableExtraPersistence 打开第二份持久化副本，用于模拟 Dwisckey。
//
// 只写不读：它存在的意义是把"每条写入多落一次盘"这个代价计入测量，
// 读路径不受影响，仍走 Raft 日志的偏移。
func (rf *Raft) EnableExtraPersistence(path string) error {
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	rf.extraLogFile = f
	rf.extraLogWriter = bufio.NewWriterSize(f, 1<<20)
	return nil
}

func (rf *Raft) CloseLogFile() {
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	if rf.logWriter != nil {
		rf.logWriter.Flush()
		rf.logWriter = nil
	}
	if rf.logFile != nil {
		rf.logFile.Close()
		rf.logFile = nil
	}
}

// SetSyncOnWrite 开关日志写入的 fsync。需在节点开始服务前调用。
func (rf *Raft) SetSyncOnWrite(v bool) {
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	rf.syncOnWrite = v
}

// SetCurrentLog 切换日志文件。version 是切换后文件对应的 GC 轮次，
// 与文件句柄一同在 logMu 下更新——这样后续写入记录的偏移和版本必然配套。
func (rf *Raft) SetCurrentLogVersioned(currentLog string, version int32) {
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	// The old file ends here: its last record is the base candidate for the new file, and
	// becomes the persisted base when the old file is deleted.
	rf.pendingBaseIndex = rf.lastWrittenIndex
	rf.pendingBaseTerm = rf.lastWrittenTerm
	rf.currentLog = currentLog
	rf.logVersion = version
	if err := rf.openLogFile(currentLog); err != nil {
		log.Fatalf("打开存储Raft日志的磁盘文件失败：%v", err)
	}
}

func (rf *Raft) SetCurrentLog(currentLog string) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// GC 会切到新文件，句柄要跟着换，否则后续写入还落在旧文件上。
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	rf.currentLog = currentLog
	if err := rf.openLogFile(currentLog); err != nil {
		log.Fatalf("打开存储Raft日志的磁盘文件失败：%v", err)
	}
}

// SetCurrentPersister is called by GC at a file switch. rf.persister is read only in
// WriteEntryToFile (for PadKey, under logMu), so the same lock guards the swap; without it
// the swap and the read race. PadKey has no instance state, so either pointer yields the
// same key, but the pointer access itself still needs synchronisation.
func (rf *Raft) SetCurrentPersister(persister *Persister) {
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	rf.persister = persister
}

// WriteEntryToFile 将条目追加到当前日志文件。
//
// 不接受文件名参数：目标文件由 rf.currentLog 决定，而它归 logMu 管。让调用方
// 传 rf.currentLog 意味着在锁外读这个字段——GC 正好会在自己的 goroutine 里改它，
// 那是一个 -race 能抓到的真实数据竞争（调用方读，SetCurrentLog 写，两把不同的锁）。
//
// 调用方需持有 rf.mu：本函数改写 rf.Offsets。
func (rf *Raft) WriteEntryToFile(e []*Entry, startPos int64) {
	// 与 SetCurrentLog 互斥：GC 换文件时会关掉当前句柄。
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	// 句柄常驻，不再每条 OpenFile/Close。首次调用时打开。
	if rf.logWriter == nil {
		if err := rf.openLogFile(rf.currentLog); err != nil {
			log.Fatalf("打开存储Raft日志的磁盘文件失败：%v", err)
		}
	}
	writer := rf.logWriter

	var offset int64
	var err error
	// 预分配足够大的偏移量切片，避免了在循环中动态扩容偏移量切片的操作
	offsets := make([]int64, len(e))

	if startPos == 0 { // 0 是直接追加：位置自行维护，省掉一次 Seek
		offset = rf.logOffset
	} else {
		// 同步日志时需覆盖与 leader 冲突的部分。缓冲区里可能还压着尚未落盘的
		// 追加内容，必须先刷净再回退写入位置，否则新旧数据会交错。
		if err = writer.Flush(); err != nil {
			log.Fatalf("刷新缓冲区失败：%v", err)
		}
		if _, err = rf.logFile.Seek(startPos, io.SeekStart); err != nil {
			log.Fatalf("定位存储Raft日志的磁盘文件的起始位置失败：%v", err)
		}
		offset = startPos
	}

	for i, entry := range e {

		valueSize := uint32(len(entry.Value))

		paddedKey := ""
		if !entry.NoOp {
			paddedKey = rf.persister.PadKey(entry.Key) // 存入valuelog里面也用
		}
		keySize := uint32(len(paddedKey))          // NoOp records have keySize==0; recovery relies on it
		data := make([]byte, 20+keySize+valueSize) // 48 bytes for 6 uint64 + key + value

		// 将数据编码到byte slice中
		binary.LittleEndian.PutUint32(data[0:4], entry.Index)
		binary.LittleEndian.PutUint32(data[4:8], entry.CurrentTerm)
		binary.LittleEndian.PutUint32(data[8:12], entry.VotedFor)
		binary.LittleEndian.PutUint32(data[12:16], keySize)
		binary.LittleEndian.PutUint32(data[16:20], valueSize)

		copy(data[20:20+keySize], paddedKey)
		copy(data[20+keySize:], entry.Value)

		// 写入文件
		u, err := writer.Write(data)
		if err != nil || u < len(data) {
			log.Fatalf("写入存储Raft日志的磁盘文件失败：%v", err)
		}

		// 同一份编码结果再落一次盘，供 Dwisckey 使用。写入相同的字节数，
		// 落盘代价才与真实的第二份 valuelog 相当。
		if rf.extraLogWriter != nil {
			if _, werr := rf.extraLogWriter.Write(data); werr != nil {
				log.Fatalf("写入第二份日志失败：%v", werr)
			}
		}

		// _, err = file.Write(data)
		// if err != nil {
		// 	fmt.Println("写入存储Raft日志的磁盘文件有问题")
		// }
		// 添加偏移量到数组中
		// offsets = append(offsets, offset)
		offsets[i] = offset
		offset += int64(len(data))
	}
	// Flush 只把数据交给操作系统（write 系统调用），数据落在 page cache 里，
	// 进程崩溃不丢但机器断电会丢。Raft 要求日志在响应客户端前真正落盘，
	// 那需要 Sync。
	if err = writer.Flush(); err != nil {
		log.Fatalf("刷新缓冲区失败：%v", err)
	}
	if rf.syncOnWrite {
		if err = rf.logFile.Sync(); err != nil {
			log.Fatalf("日志落盘（fsync）失败：%v", err)
		}
	}
	// Dwisckey 的第二次落盘。与主日志同在 logMu 之内、用同样的 fsync 语义，
	// 否则"多一次持久化"的代价就测不准。
	if rf.extraLogWriter != nil {
		if err = rf.extraLogWriter.Flush(); err != nil {
			log.Fatalf("刷新第二份日志缓冲区失败：%v", err)
		}
		if rf.syncOnWrite {
			if err = rf.extraLogFile.Sync(); err != nil {
				log.Fatalf("第二份日志落盘（fsync）失败：%v", err)
			}
		}
	}
	if startPos != 0 {
		// Overwrite: everything past the overwritten region is no longer part of the log
		// (the matching rf.log entries were just truncated), so the file is truncated to the
		// end of the new content. The old code moved the write position back to the previous
		// end of file, leaving stale bytes that a sequential replay would read as records.
		if err = rf.logFile.Truncate(offset); err != nil {
			log.Fatalf("截断日志文件失败：%v", err)
		}
		if rf.syncOnWrite {
			if err = rf.logFile.Sync(); err != nil {
				log.Fatalf("截断后落盘失败：%v", err)
			}
		}
	}
	rf.logOffset = offset
	if n := len(e); n > 0 {
		rf.lastWrittenIndex = int(e[n-1].Index)
		rf.lastWrittenTerm = int32(e[n-1].CurrentTerm)
	}

	rf.Offsets = append(rf.Offsets, offsets...)
	// 版本与偏移同批追加：此刻仍持有 logMu，logVersion 必定是刚写进去的那个文件的。
	for range offsets {
		rf.offsetVersions = append(rf.offsetVersions, rf.logVersion)
	}
}

// ReadValueFromFile 从指定的偏移量读取value
func (rf *Raft) ReadValueFromFile(filename string, offset int64) (string, string, error) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// 打开文件
	file, err := os.Open(filename)
	if err != nil {
		return "", "", err
	}
	defer file.Close()

	if offset == -1 {
		return "NOKEY", "", nil
	}

	// 移动到指定偏移量
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		fmt.Println("get时，seek文件的位置有问题")
		return "", "", err
	}

	// 读取数据到buffer中，首先是固定长度的20字节
	header := make([]byte, 20)

	n, err := file.Read(header)
	// fmt.Printf("读取了几个字节的数据%v\n",n)
	if err != nil {
		fmt.Println("get时，读取key和value的前20个固定字节时有问题")
		return "", "", err
	}
	// 确保读取的字节数足够
	if n < 20 {
		fmt.Printf("not enough data: expected 20 bytes, got %d\n", n)
		return "", "", err
	}

	// 解析固定长度的字段
	keySize := binary.LittleEndian.Uint32(header[12:16])
	valueSize := binary.LittleEndian.Uint32(header[16:20])

	// 读取Key和Value
	keyValueBuffer := make([]byte, keySize+valueSize)
	if _, err := file.Read(keyValueBuffer); err != nil {
		return "", "", err
	}

	// Key是从buffer的开始部分
	key := string(keyValueBuffer[:keySize])
	// Value是紧跟在Key后面的部分
	value := string(keyValueBuffer[keySize:])

	return key, value, nil
}
