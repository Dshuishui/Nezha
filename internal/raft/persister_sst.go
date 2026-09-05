package raft

import (
	"fmt"
	"path/filepath"
	"sort"

	"github.com/linxGnu/grocksdb"
)

// SSTable export and import for the LSM-Raft baseline.
//
// RocksDB only ingests files produced by SstFileWriter (its own flush output lacks the
// external-file version property and is refused with "External file version not
// found"), so the leader materialises each span itself: the rows applied in the span,
// last version per key, plus the applied-index marker set to the span's last index.
// Ingesting such a file is equivalent to replaying the span's entries, and the marker
// moves the follower's applied index together with the data.

// WriteSpanSST writes rows (store keys already padded, values as stored) and the applied
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
