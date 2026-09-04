package raft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"gitee.com/dong-shuishui/FlexSync/rpc/raftrpc"
	"gitee.com/dong-shuishui/FlexSync/util"
)

// LogFile 是恢复时要扫的一个日志文件：路径 + 它对应的 GC 版本号（写进 offsetVersions）。
type LogFile struct {
	Path    string
	Version int32
}

// 单条记录的大小上限，只用来识别损坏的头部；正常记录远小于此。
const (
	maxRecordKey   = 1 << 16
	maxRecordValue = 1 << 30
	recordHeader   = 20
)

// RecoverLog 用磁盘上仍保留的日志文件重建内存里的 Raft 日志。
//
// 文件按从旧到新给出，记录的 index 必须连续；第一条记录的 index-1 即日志基址，
// 需与状态文件里的 BaseIndex 一致，否则视为状态不一致直接报错——不猜。
// index > lastApplied 的记录重新进入 Offsets/offsetVersions 队列，等 apply 循环消费；
// 之前的只留在 rf.log 里供一致性检查用，第一次 compactLog 会把它们裁掉。
//
// 只有最后一个文件的最后一条允许不完整（崩溃时正写到一半），此时把文件截断到该条起点。
// 其他任何位置的不完整或不连续都是错误。
//
// 调用方保证此时各 loop 尚未启动，无需加锁。
func (rf *Raft) RecoverLog(files []LogFile, lastApplied int) (lastIndex int, err error) {
	var entries []*raftrpc.LogEntry
	var offsets []int64
	var versions []int32
	expected := -1
	base := 0

	for fi, lf := range files {
		isLast := fi == len(files)-1
		f, err := os.OpenFile(lf.Path, os.O_RDWR, 0666)
		if err != nil {
			return 0, fmt.Errorf("open %s: %v", lf.Path, err)
		}
		r := bufio.NewReaderSize(f, 1<<20)
		var offset int64
		hdr := make([]byte, recordHeader)
		for {
			n, rerr := io.ReadFull(r, hdr)
			if rerr == io.EOF && n == 0 {
				break // 正好读到文件尾
			}
			var ks, vs uint32
			if rerr == nil {
				ks = binary.LittleEndian.Uint32(hdr[12:16])
				vs = binary.LittleEndian.Uint32(hdr[16:20])
				if ks > maxRecordKey || vs > maxRecordValue {
					rerr = fmt.Errorf("record header at %d looks corrupt (key=%d value=%d)", offset, ks, vs)
				}
			}
			var body []byte
			if rerr == nil {
				body = make([]byte, int(ks)+int(vs))
				_, rerr = io.ReadFull(r, body)
			}
			if rerr != nil {
				if !isLast {
					f.Close()
					return 0, fmt.Errorf("%s: incomplete record at offset %d in a non-final log file: %v", lf.Path, offset, rerr)
				}
				util.EPrintf("RaftNode[%d] 恢复：%s 末尾有一条不完整记录（offset=%d: %v），截断到该记录起点", rf.me, lf.Path, offset, rerr)
				if terr := f.Truncate(offset); terr != nil {
					f.Close()
					return 0, fmt.Errorf("truncate %s to %d: %v", lf.Path, offset, terr)
				}
				if serr := f.Sync(); serr != nil {
					f.Close()
					return 0, serr
				}
				break
			}
			index := int(binary.LittleEndian.Uint32(hdr[0:4]))
			term := int32(binary.LittleEndian.Uint32(hdr[4:8]))
			if expected == -1 {
				expected = index
				base = index - 1
			} else if index != expected {
				f.Close()
				return 0, fmt.Errorf("%s: log not contiguous at offset %d: got index %d, want %d", lf.Path, offset, index, expected)
			}
			cmd := &raftrpc.DetailCod{Index: int32(index), Term: term}
			if ks == 0 {
				cmd.OpType = "TermLog" // 空指令标记：key 长度为 0，正常记录的 key 恒为 KeyLength
			} else {
				cmd.OpType = "Put"
				cmd.Key = rf.persister.UnpadKey(string(body[:ks]))
				cmd.Value = string(body[ks:])
			}
			entries = append(entries, &raftrpc.LogEntry{Term: term, Command: cmd})
			if index > lastApplied {
				offsets = append(offsets, offset)
				versions = append(versions, lf.Version)
			}
			expected++
			offset += int64(recordHeader) + int64(ks) + int64(vs)
		}
		f.Close()
	}

	if expected == -1 {
		// 一条记录都没有：允许，只要 lastApplied 也是 0 且基址为 0（空集群重启）
		if lastApplied != 0 || rf.lastIncludedIndex != 0 {
			return 0, errors.New("log files are empty but applied index / base index are not zero")
		}
		return 0, nil
	}
	if rf.stateLoaded && rf.lastIncludedIndex != base {
		return 0, fmt.Errorf("log base mismatch: state file says %d, first log record implies %d", rf.lastIncludedIndex, base)
	}
	lastIndex = expected - 1
	if lastApplied < base || lastApplied > lastIndex {
		return 0, fmt.Errorf("applied index %d outside recovered log range (%d, %d]", lastApplied, base, lastIndex)
	}

	rf.log = entries
	rf.lastIncludedIndex = base
	rf.Offsets = offsets
	rf.offsetVersions = versions
	rf.lastApplied = lastApplied
	rf.commitIndex = lastApplied
	rf.shotOffset = lastApplied // 不变式：Offsets[0] 对应 index lastApplied+1
	util.DPrintf("RaftNode[%d] 恢复完成：log (%d, %d]，%d 条待 apply，term=%d votedFor=%d",
		rf.me, base, lastIndex, len(offsets), rf.currentTerm, rf.votedFor)
	return lastIndex, nil
}
