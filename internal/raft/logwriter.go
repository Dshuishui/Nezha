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
	// Seek 对写入位置完全无效。冲突覆盖（OverwriteLogFileFrom）依赖 Seek 回退，在
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

// SetCurrentPersister is called by GC at a file switch. The swap happens under logMu
// because that is the lock the log-writing path holds; the pointer access itself needs
// synchronising even though no reader now derives anything from the persister's identity.
// (writeEntries used to call rf.persister.PadKey here, which is what originally made
// the race visible. Keys are stored verbatim now, but the lock discipline still stands.)
func (rf *Raft) SetCurrentPersister(persister *Persister) {
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	rf.persister = persister
}

// AppendToLogFile 把条目接在当前日志文件的末尾。调用方需持有 rf.mu（本函数改写 rf.Offsets）。
func (rf *Raft) AppendToLogFile(e []*Entry) {
	rf.writeEntries(e, 0, false)
}

// OverwriteLogFileFrom 回退到 startPos 覆盖写，并把 startPos 之后的字节全部截掉。
// follower 发现与 leader 冲突时走这里：冲突点及其之后的条目已经不属于日志了。
// 调用方需持有 rf.mu。
func (rf *Raft) OverwriteLogFileFrom(e []*Entry, startPos int64) {
	if startPos < 0 {
		log.Fatalf("覆盖写的起始位置不能为负：%d", startPos)
	}
	rf.writeEntries(e, startPos, true)
}

// writeEntries 是两者共同的实现。
//
// 不接受文件名参数：目标文件由 rf.currentLog 决定，而它归 logMu 管。让调用方
// 传 rf.currentLog 意味着在锁外读这个字段——GC 正好会在自己的 goroutine 里改它，
// 那是一个 -race 能抓到的真实数据竞争（调用方读，SetCurrentLog 写，两把不同的锁）。
//
// 「追加」还是「覆盖」由 overwrite 这个参数决定，**不能**从 startPos 的取值去猜。
// 原先是一个方法两用：startPos == 0 当作追加，非 0 当作覆盖。可 0 本身就是一个
// 合法的覆盖位置——日志文件的第一条记录正好在偏移 0。于是"follower 在文件第一条
// 记录处发现冲突"这一情形被当成了追加：新记录接在旧记录后面，覆盖点之后的陈旧字节
// 一个都没截掉。活着的时候读是对的（rf.Offsets 指向新写的那一份），但重启时
// RecoverLog 顺序回放会先撞上旧记录，索引不连续，节点直接起不来：
//
//	RaftState.log: log not contiguous at offset 69: got index 1, want 4
//
// 而这个偏移在两种寻常情况下就是 0：全新节点收到的第一条，以及 GC 换文件之后写进
// 新文件的第一条（SetCurrentLog 把 logOffset 归零）。都不是边角情形。
func (rf *Raft) writeEntries(e []*Entry, startPos int64, overwrite bool) {
	// 与 SetCurrentLog 互斥：GC 换文件时会关掉当前句柄。
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	recordLogWrite(len(e))
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

	if !overwrite { // 追加：位置自行维护，省掉一次 Seek
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
			paddedKey = entry.Key // 存储层原样存 key，见 persister.go 的契约说明
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
	if overwrite {
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
		// 已落盘的前沿。commit 判据拿它当 leader 自己的一票（见 durableIndex）：
		// 攒批模式下 rf.log 领先于磁盘，拿 rf.log 的末尾投票等于用一份还没落盘的
		// 副本凑多数派。覆盖写（冲突截断）会让这个值**变小**，那是对的——
		// 截断点之后的字节已经不属于日志了，所以无条件存。
		rf.persistedIndex.Store(int64(e[n-1].Index))
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

// LogCut 是当前日志文件的一个一致切点，用来构造快照。
//
// 为什么不能直接 os.Stat 那个文件：写入走 bufio，缓冲里的字节还不在文件里，而且一次写入
// 可能只落了半条记录。两者都会让接收端拿到一个尾部撕裂的日志——recovery 能容忍最后一个
// 文件末尾的半条记录（截掉它），但那是崩溃恢复的兜底，不该是正常传输的常态。
type LogCut struct {
	Path  string // 当前日志文件
	Bytes int64  // 前这么多字节已刷净且按记录对齐
	// BaseIndex / BaseTerm 是这个文件第一条记录之前那一条的 index/term，也就是快照的
	// lastIncludedIndex/Term：比它更早的数据已经被 GC 搬进分区文件，不在日志里了。
	BaseIndex int
	BaseTerm  int32
	// LastIndex / LastTerm 是切点处最后一条记录。接收端装完快照之后日志就到这里，
	// leader 于是从 LastIndex+1 继续正常复制。
	LastIndex int
	LastTerm  int32
}

// CutLog 刷净日志缓冲并返回一个记录对齐的切点。
//
// fsync 一并做掉：接收端要按这个长度去读文件，而"已经交给内核但还没落盘"的字节在断电后
// 可能不存在。这一次 fsync 只发生在制作快照时，不在写路径上。
func (rf *Raft) CutLog() (LogCut, error) {
	rf.logMu.Lock()
	if rf.logWriter != nil {
		if err := rf.logWriter.Flush(); err != nil {
			rf.logMu.Unlock()
			return LogCut{}, fmt.Errorf("flush log: %w", err)
		}
	}
	if rf.logFile != nil {
		if err := rf.logFile.Sync(); err != nil {
			rf.logMu.Unlock()
			return LogCut{}, fmt.Errorf("fsync log: %w", err)
		}
	}
	cut := LogCut{
		Path:      rf.currentLog,
		Bytes:     rf.logOffset,
		LastIndex: rf.lastWrittenIndex,
		LastTerm:  rf.lastWrittenTerm,
	}
	rf.logMu.Unlock()

	// fileBaseIndex 在 rf.mu 下，不在 logMu 下。分两段取而不是嵌套加锁，与
	// PersistLogBase 的做法一致（它也是先 logMu 后 rf.mu，顺序执行不嵌套）。
	// 两段之间可能夹进一次 GC 切换，所以调用方在用完之后必须复核轮次没有变——
	// 见 createSnapshot 的收尾检查。
	rf.mu.Lock()
	cut.BaseIndex, cut.BaseTerm = rf.fileBaseIndex, rf.fileBaseTerm
	rf.mu.Unlock()
	return cut, nil
}

// flushBatchLog 把一次 AppendEntries 攒下的条目一次写进日志文件。调用方须持有 rf.mu。
//
// 之前这批条目是**逐条**落盘的，而代码写的是"批量存储"。判据是：
//
//	rf.batchLog = append(rf.batchLog, &entry)
//	if index == rf.lastIndex() { ...写盘并清空... }
//
// 紧挨上一行的 rf.appendLog 刚把这条追进内存日志，于是 rf.lastIndex() 必然就等于
// index——这个条件恒为真，攒批从来没发生过，每条都走一次 Flush + fsync。
// leader 那边是按**编码字节数**打包的（doAppendEntries 里 totalSize >= threshold 才截断），
// 所以一次 AppendEntries 常常带几十上百条，follower 却为此做了同样多次 fsync。
// 改成循环结束后统一刷一次，与 leader 侧 groupcommit 的口径一致；
// 应答前落盘这条 Raft 要求不变，因为刷盘仍在 reply.Success = true 之前。
func (rf *Raft) flushBatchLog() {
	if len(rf.batchLog) == 0 {
		return
	}
	rf.AppendToLogFile(rf.batchLog)
	rf.batchLog = rf.batchLog[:0] // 复用底层数组
}
