package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/util"
)

// File transport from the leader to a follower, carrying state-machine files.
//
// Two kinds of payload travel over it, told apart by SSTableSpan.Kind:
//
//   - SPAN (the LSM-Raft baseline): the files hold the leader's state changes for log
//     indexes [Start, End]. A follower that ingests them skips replaying those entries
//     into its own LSM-tree (no WAL, no memtable, no flush), which is the follower-side
//     saving LSM-Raft describes. The Raft log itself is unchanged: every entry is still
//     replicated and persisted on every node.
//   - SNAPSHOT: the files hold the complete state as of End. This is the role
//     InstallSnapshot plays in standard Raft, and the only way a follower that has
//     fallen behind the leader's compaction point can ever catch up.
//
// The two share everything below because the hard parts -- chunking a large file set
// over a client-streaming RPC, reassembling it by name and offset, the term rules, a
// transfer deadline that scales with the payload -- are the same for both. What differs
// is one requirement on the receiver, and that is the reason Kind is an explicit field
// rather than a convention over the index fields: a span requires the follower to be
// applied through Start-1 (otherwise it answers GAP), a snapshot requires nothing at
// all. Requiring nothing is the entire point of a snapshot.
//
// The Raft layer only moves bytes and checks terms. What to do with a payload (ingest,
// install, skip, report a gap) is the state machine's decision, injected through
// SSTableInstaller.

// SSTableSpan is one shipped unit. Which fields carry meaning depends on Kind; the
// per-field comments say which, and the installer must dispatch on Kind rather than
// guess from the values.
type SSTableSpan struct {
	Kind raftrpc.InstallSSTableKind
	// Start is the first log index the files cover, inclusive. SPAN only.
	Start int
	// End is the last log index the files cover, inclusive. For SNAPSHOT this is the
	// snapshot's lastIncludedIndex.
	End   int
	Files []string // absolute paths, ingestion order
	// OldestAvailable is the Start of the oldest span the leader can still send. A
	// follower that is behind it must replay entries up to OldestAvailable-1 itself.
	// SPAN only.
	OldestAvailable int
	// LastIncludedTerm is the term of the entry at End. SNAPSHOT only, and required
	// there: End becomes the follower's log base, so it has to be able to answer a
	// later AppendEntries consistency check at that index.
	LastIncludedTerm int
}

// dirName is where a transfer is reassembled under the incoming directory. The two kinds
// get distinct names so that a directory left behind by a crash says what it was: a
// half-received snapshot must be discarded and re-requested, not mistaken for a span.
func (s SSTableSpan) dirName() string {
	if s.Kind == raftrpc.InstallSSTableKind_SNAPSHOT {
		return fmt.Sprintf("snapshot-%d", s.End)
	}
	return fmt.Sprintf("%d-%d", s.Start, s.End)
}

// SSTableInstaller is implemented by the state machine. The files have been written to
// local disk by the transport; the installer owns them from here on. It must dispatch on
// span.Kind: a state machine that only understands one kind has to answer FAILED for the
// other rather than install it under the wrong semantics.
type SSTableInstaller func(span SSTableSpan) (applied int, status raftrpc.InstallSSTableStatus)

// SetSSTableInstaller enables the InstallSSTable RPC. incomingDir receives the files of a
// transfer while it is in flight (one subdirectory per transfer, named by dirName).
func (rf *Raft) SetSSTableInstaller(incomingDir string, fn SSTableInstaller) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.sstIncomingDir = incomingDir
	rf.sstInstaller = fn
}

const sstChunkSize = 1 << 20

// InstallSSTable is the follower side. It reassembles the files under
// <incomingDir>/<dirName>/ and hands them to the installer once the last chunk arrives.
func (rf *Raft) InstallSSTable(stream raftrpc.Raft_InstallSSTableServer) error {
	var (
		dir      string
		files    = map[string]*os.File{} // by basename
		order    []string                // by FileSeq
		span     SSTableSpan
		header   *raftrpc.InstallSSTableRequest
		received int64
	)
	cleanup := func() {
		for _, f := range files {
			f.Close()
		}
		if dir != "" {
			os.RemoveAll(dir)
		}
	}
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			cleanup()
			return errors.New("InstallSSTable: stream closed before the last chunk")
		}
		if err != nil {
			cleanup()
			return err
		}
		if header == nil {
			header = req
			resp, ok := rf.checkSSTableTerm(req)
			if !ok {
				cleanup()
				return stream.SendAndClose(resp)
			}
			rf.mu.Lock()
			incoming, installer := rf.sstIncomingDir, rf.sstInstaller
			rf.mu.Unlock()
			if installer == nil {
				cleanup()
				return stream.SendAndClose(&raftrpc.InstallSSTableResponse{
					Term: req.Term, Status: raftrpc.InstallSSTableStatus_FAILED})
			}
			span = SSTableSpan{Kind: req.Kind,
				Start: int(req.SpanStart), End: int(req.SpanEnd),
				OldestAvailable:  int(req.OldestAvailable),
				LastIncludedTerm: int(req.LastIncludedTerm)}
			dir = filepath.Join(incoming, span.dirName())
			os.RemoveAll(dir) // a previous attempt at the same transfer left half of it here
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return err
			}
			order = make([]string, req.FileCount)
		}
		if req.FileName != "" {
			f, ok := files[req.FileName]
			if !ok {
				f, err = os.OpenFile(filepath.Join(dir, filepath.Base(req.FileName)),
					os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
				if err != nil {
					cleanup()
					return err
				}
				files[req.FileName] = f
				if int(req.FileSeq) < len(order) {
					order[req.FileSeq] = f.Name()
				}
			}
			if len(req.Data) > 0 {
				if _, err := f.WriteAt(req.Data, req.Offset); err != nil {
					cleanup()
					return err
				}
				received += int64(len(req.Data))
			}
		}
		if req.Last {
			break
		}
	}
	for name, f := range files {
		if err := f.Sync(); err != nil {
			cleanup()
			return fmt.Errorf("sync %s: %w", name, err)
		}
		f.Close()
	}
	files = nil
	for i, p := range order {
		if p == "" {
			os.RemoveAll(dir)
			return fmt.Errorf("InstallSSTable: %s %s is missing file %d of %d",
				span.Kind, span.dirName(), i, len(order))
		}
	}
	span.Files = order
	rf.mu.Lock()
	installer := rf.sstInstaller
	rf.mu.Unlock()
	applied, status := installer(span)
	util.DPrintf("RaftNode[%d] InstallSSTable %s[%s] files=%d bytes=%d -> %s applied=%d",
		rf.me, span.Kind, span.dirName(), len(order), received, status, applied)
	if status != raftrpc.InstallSSTableStatus_INGESTED {
		os.RemoveAll(dir) // ingestion with MoveFiles consumed the files on success
	}
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()
	return stream.SendAndClose(&raftrpc.InstallSSTableResponse{
		Term: int32(term), Applied: int64(applied), Status: status})
}

// checkSSTableTerm applies Raft's term rules to the span header. A stale leader is
// refused; a newer term makes this node a follower of it.
func (rf *Raft) checkSSTableTerm(req *raftrpc.InstallSSTableRequest) (*raftrpc.InstallSSTableResponse, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if req.Term < int32(rf.currentTerm) {
		return &raftrpc.InstallSSTableResponse{Term: int32(rf.currentTerm),
			Status: raftrpc.InstallSSTableStatus_STALE_TERM}, false
	}
	if req.Term > int32(rf.currentTerm) {
		rf.currentTerm = int(req.Term)
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.persistHardState()
	}
	rf.leaderId = int(req.LeaderId)
	rf.lastActiveTime = time.Now()
	return nil, true
}

// SendSSTable streams one transfer to a peer and returns the follower's answer. The
// caller (the leader's state machine) decides what to do with GAP and STALE_TERM.
//
// The files are read straight from wherever they already live -- no local copy is made.
// For a snapshot that means the caller must keep them from being deleted for the whole
// duration of the call (partition files are pinned for exactly this reason); a copy of a
// multi-gigabyte state would cost more than the transfer.
func (rf *Raft) SendSSTable(peerId int, span SSTableSpan) (*raftrpc.InstallSSTableResponse, error) {
	rf.mu.Lock()
	term, me := rf.currentTerm, rf.me
	rf.mu.Unlock()

	var total int64
	for _, p := range span.Files {
		st, err := os.Stat(p)
		if err != nil {
			return nil, err
		}
		total += st.Size()
	}
	conn, err := rf.pools[peerId].Get()
	if err != nil {
		return nil, fmt.Errorf("SendSSTable: no conn to %s: %w", rf.peers[peerId], err)
	}
	defer conn.Close()
	// Budget: 30 s plus one second per 8 MB, so a slow link never trips the deadline
	// before a large span is through.
	timeout := 30*time.Second + time.Duration(total/(8<<20))*time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	stream, err := raftrpc.NewRaftClient(conn.Value()).InstallSSTable(ctx)
	if err != nil {
		return nil, err
	}
	newMsg := func(name string, seq int32, off int64, data []byte, last bool) *raftrpc.InstallSSTableRequest {
		return &raftrpc.InstallSSTableRequest{
			Term: int32(term), LeaderId: int32(me), Kind: span.Kind,
			SpanStart: int64(span.Start), SpanEnd: int64(span.End),
			OldestAvailable:  int64(span.OldestAvailable),
			LastIncludedTerm: int32(span.LastIncludedTerm),
			FileCount:        int32(len(span.Files)),
			FileName:         name, FileSeq: seq, Offset: off, Data: data, Last: last,
		}
	}
	buf := make([]byte, sstChunkSize)
	for seq, p := range span.Files {
		f, err := os.Open(p)
		if err != nil {
			return nil, err
		}
		var off int64
		for {
			n, rerr := f.Read(buf)
			if n > 0 {
				if err := stream.Send(newMsg(filepath.Base(p), int32(seq), off, buf[:n], false)); err != nil {
					f.Close()
					return nil, err
				}
				off += int64(n)
			}
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				f.Close()
				return nil, rerr
			}
		}
		f.Close()
		if off == 0 {
			// An empty file still has to be created on the far side.
			if err := stream.Send(newMsg(filepath.Base(p), int32(seq), 0, nil, false)); err != nil {
				return nil, err
			}
		}
	}
	if err := stream.Send(newMsg("", 0, 0, nil, true)); err != nil {
		return nil, err
	}
	return stream.CloseAndRecv()
}
