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

// LogFile is one log file to replay during recovery: its path and the GC file version its
// offsets belong to (recorded into offsetVersions).
type LogFile struct {
	Path    string
	Version int32
}

// Record size limits used only to recognise a corrupt header; real records are far smaller.
const (
	maxRecordKey   = 1 << 16
	maxRecordValue = 1 << 30
	recordHeader   = 20
)

// RecoverLog rebuilds the in-memory Raft log from the log files still on disk.
//
// Files are given oldest first and their record indices must be contiguous. The first
// record's index minus one is the log base; it must match BaseIndex from the state file,
// otherwise the state is inconsistent and recovery fails rather than guessing. Records with
// index > lastApplied re-enter the Offsets/offsetVersions queue for the apply loop; earlier
// ones stay in rf.log for consistency checks only, and the first compactLog trims them.
//
// Only the final record of the last file may be incomplete (a crash mid-write); it is
// truncated away. An incomplete record anywhere else, or a gap, is an error.
//
// The caller guarantees that no loop is running yet, so no locking is needed.
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
				break // clean end of file
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
				util.EPrintf("RaftNode[%d] recovery: %s ends with an incomplete record (offset=%d: %v); truncating to the record start", rf.me, lf.Path, offset, rerr)
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
				cmd.OpType = "TermLog" // no-op marker: zero-length key; real keys are always KeyLength bytes
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
		// No records at all is fine only for an empty cluster restart.
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
	rf.shotOffset = lastApplied // invariant: Offsets[0] belongs to index lastApplied+1
	util.DPrintf("RaftNode[%d] recovery complete: log (%d, %d], %d entries pending apply, term=%d votedFor=%d",
		rf.me, base, lastIndex, len(offsets), rf.currentTerm, rf.votedFor)
	return lastIndex, nil
}
