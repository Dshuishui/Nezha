package raft

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/linxGnu/grocksdb"
)

// SSTable export and import.
//
// RocksDB only ingests files produced by SstFileWriter (its own flush output lacks the
// external-file version property and is refused with "External file version not
// found"), so whatever is shipped has to be materialised deliberately. Two things are:
//
//   - one span (the LSM-Raft baseline): the rows applied in the span, last version per
//     key, plus the applied-index marker set to the span's last index. Ingesting such a
//     file is equivalent to replaying the span's entries, and the marker moves the
//     follower's applied index together with the data.
//   - the whole store (a snapshot for a replica that has fallen too far behind).
//
// WriteSpanSST writes rows (store keys as stored, values as stored) and the applied
// index marker to path as an ingestible SSTable.
func (p *Persister) WriteSpanSST(path string, rows map[string][]byte, applied int) error {
	keys := make([]string, 0, len(rows)+1)
	for k := range rows {
		keys = append(keys, k)
	}
	keys = append(keys, appliedIndexKey)
	sort.Strings(keys) // SstFileWriter requires ascending key order (bytewise comparator)

	envOpts := grocksdb.NewDefaultEnvOptions()
	defer envOpts.Destroy()
	dbOpts := grocksdb.NewDefaultOptions()
	defer dbOpts.Destroy()
	w := grocksdb.NewSSTFileWriter(envOpts, dbOpts)
	defer w.Destroy()
	if err := w.Open(path); err != nil {
		return fmt.Errorf("open %s: %w", filepath.Base(path), err)
	}
	for _, k := range keys {
		v := rows[k]
		if k == appliedIndexKey {
			v = encodeApplied(applied)
		}
		if err := w.Put([]byte(k), v); err != nil {
			return fmt.Errorf("put %q: %w", k, err)
		}
	}
	if err := w.Finish(); err != nil {
		return fmt.Errorf("finish %s: %w", filepath.Base(path), err)
	}
	return nil
}

// IngestSSTables adds span files to the store as the newest data. Files are moved
// (hard-linked) into the store, so the caller's copies are consumed.
func (p *Persister) IngestSSTables(paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	opts := grocksdb.NewDefaultIngestExternalFileOptions()
	defer opts.Destroy()
	opts.SetMoveFiles(true)
	opts.SetAllowGlobalSeqNo(true)
	opts.SetAllowBlockingFlush(true)
	// Files of one span may overlap in key range, and RocksDB refuses overlapping files
	// in a single call, so ingest them one at a time in order: each call receives a
	// higher sequence number than the previous one.
	for _, path := range paths {
		if err := p.db.IngestExternalFile([]string{path}, opts); err != nil {
			return fmt.Errorf("ingest %s: %w", filepath.Base(path), err)
		}
	}
	return nil
}

// ExportStoreSST writes the entire store to one ingestible SST in ascending key order,
// and returns how many rows it wrote and the applied index that export corresponds to.
//
// The export runs against a RocksDB snapshot handle, so the data rows and the applied
// marker come from one read view. That is the property a shipped snapshot needs and the
// easiest one to get wrong: a manifest whose index does not match its data installs a
// state machine that claims to be somewhere it is not. TiKV asserts the same thing in
// do_snapshot. It is also why applied is a return value rather than a parameter -- the
// caller should not have the opportunity to name an index the data does not support.
//
// Rows are streamed straight into the writer, never accumulated: WriteSpanSST's map
// parameter is fine for one span, but the store has a row per live key, which at
// 100GB / 64B values is of the order of a billion.
//
// An empty store produces no file at all (rows == 0, path not created): SstFileWriter
// refuses to finish a file with no entries, and there is nothing for the receiver to do.
func (p *Persister) ExportStoreSST(path string) (rows int, applied int, err error) {
	snap := p.db.NewSnapshot()
	defer p.db.ReleaseSnapshot(snap)
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	ro.SetSnapshot(snap)
	ro.SetFillCache(false) // 一次性全量扫，不要把块缓存冲掉

	envOpts := grocksdb.NewDefaultEnvOptions()
	defer envOpts.Destroy()
	dbOpts := grocksdb.NewDefaultOptions()
	defer dbOpts.Destroy()
	w := grocksdb.NewSSTFileWriter(envOpts, dbOpts)
	defer w.Destroy()
	if err := w.Open(path); err != nil {
		return 0, 0, fmt.Errorf("open %s: %w", filepath.Base(path), err)
	}

	it := p.db.NewIterator(ro)
	defer it.Close()
	// appliedIndexKey 以 0x00 开头，而用户 key 不允许以 0x00 开头（见 ValidateKey），
	// 所以升序迭代天然先给出 applied 标记，不必为它的位置做特别处理——SstFileWriter
	// 要求严格升序，顺序错了它直接报错，不会静默写出一份读不回来的文件。
	for it.SeekToFirst(); it.Valid(); it.Next() {
		k, v := it.Key(), it.Value()
		key, val := k.Data(), v.Data()
		if string(key) == appliedIndexKey && len(val) == 8 {
			applied = int(binary.LittleEndian.Uint64(val))
		}
		werr := w.Put(key, val)
		k.Free()
		v.Free()
		if werr != nil {
			return 0, 0, fmt.Errorf("export row %d: %w", rows, werr)
		}
		rows++
	}
	if err := it.Err(); err != nil {
		return 0, 0, fmt.Errorf("iterate store: %w", err)
	}
	if rows == 0 {
		os.Remove(path)
		return 0, 0, nil
	}
	if err := w.Finish(); err != nil {
		return 0, 0, fmt.Errorf("finish %s: %w", filepath.Base(path), err)
	}
	return rows, applied, nil
}
