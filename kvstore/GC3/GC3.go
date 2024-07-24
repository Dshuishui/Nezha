package GC3

import (
    "encoding/binary"
    "sort"
    "os"
    "bufio"
    "io"
    "github.com/tecbot/gorocksdb"
)

type Entry struct {
    Index       uint32
    CurrentTerm uint32
    VotedFor    uint32
    Key         string
    Value       []byte
    Offset      int64
}

func CompactAndSort(oldFilename, newFilename string, db *gorocksdb.DB) error {
    // 1. 读取原文件中的所有 Entry
    entries, err := readAllEntries(oldFilename)
    if err != nil {
        return err
    }

    // 2. 去重并保留已 apply 的 Entry
    deduplicatedEntries := deduplicateEntries(entries, db)

    // 3. 排序
    sort.Slice(deduplicatedEntries, func(i, j int) bool {
        return deduplicatedEntries[i].Key < deduplicatedEntries[j].Key
    })

    // 4. 写入新文件并更新 RocksDB
    err = writeEntriesAndUpdateDB(deduplicatedEntries, newFilename, db)
    if err != nil {
        return err
    }

    return nil
}

func readAllEntries(filename string) ([]Entry, error) {
    file, err := os.Open(filename)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    var entries []Entry
    reader := bufio.NewReader(file)

    for {
        entry, offset, err := readEntry(file, reader)
        if err != nil {
            if err == io.EOF {
                break
            }
            return nil, err
        }
        entry.Offset = offset
        entries = append(entries, entry)
    }

    return entries, nil
}

func readEntry(file *os.File, reader *bufio.Reader) (Entry, int64, error) {
    var entry Entry
    var err error

    entry.Offset, err = file.Seek(0, io.SeekCurrent)
    if err != nil {
        return Entry{}, 0, err
    }

    headerData := make([]byte, 20)
    _, err = io.ReadFull(reader, headerData)
    if err != nil {
        return Entry{}, 0, err
    }

    entry.Index = binary.LittleEndian.Uint32(headerData[0:4])
    entry.CurrentTerm = binary.LittleEndian.Uint32(headerData[4:8])
    entry.VotedFor = binary.LittleEndian.Uint32(headerData[8:12])
    keySize := binary.LittleEndian.Uint32(headerData[12:16])
    valueSize := binary.LittleEndian.Uint32(headerData[16:20])

    keyData := make([]byte, keySize)
    _, err = io.ReadFull(reader, keyData)
    if err != nil {
        return Entry{}, 0, err
    }
    entry.Key = string(keyData)

    entry.Value = make([]byte, valueSize)
    _, err = io.ReadFull(reader, entry.Value)
    if err != nil {
        return Entry{}, 0, err
    }

    return entry, entry.Offset, nil
}

func deduplicateEntries(entries []Entry, db *gorocksdb.DB) []Entry {
    keyMap := make(map[string]Entry)

    for _, entry := range entries {
        // 使用 NewDefaultReadOptions() 替代 NewReadOptions()
        ro := gorocksdb.NewDefaultReadOptions()
        defer ro.Destroy()

        offsetBytes, err := db.Get(ro, []byte(entry.Key))
        if err != nil {
            continue // 如果发生错误，跳过这个 Entry
        }
        defer offsetBytes.Free()

        if offsetBytes.Size() > 0 {
            storedOffset := int64(binary.LittleEndian.Uint64(offsetBytes.Data()))
            if storedOffset == entry.Offset {
                keyMap[entry.Key] = entry
            }
        }
    }

    deduplicatedEntries := make([]Entry, 0, len(keyMap))
    for _, entry := range keyMap {
        deduplicatedEntries = append(deduplicatedEntries, entry)
    }

    return deduplicatedEntries
}

func writeEntriesAndUpdateDB(entries []Entry, filename string, db *gorocksdb.DB) error {
    file, err := os.Create(filename)
    if err != nil {
        return err
    }
    defer file.Close()

    writer := bufio.NewWriter(file)

    wo := gorocksdb.NewDefaultWriteOptions()
    defer wo.Destroy()

    for _, entry := range entries {
        newOffset, err := writeEntry(file, writer, entry)
        if err != nil {
            return err
        }

        // 更新 RocksDB 中的偏移量
        offsetBytes := make([]byte, 8)
        binary.LittleEndian.PutUint64(offsetBytes, uint64(newOffset))
        err = db.Put(wo, []byte(entry.Key), offsetBytes)
        if err != nil {
            return err
        }
    }

    return writer.Flush()
}

func writeEntry(file *os.File, writer *bufio.Writer, entry Entry) (int64, error) {
    offset, err := file.Seek(0, io.SeekCurrent)
    if err != nil {
        return 0, err
    }

    data := make([]byte, 20+len(entry.Key)+len(entry.Value))
    binary.LittleEndian.PutUint32(data[0:4], entry.Index)
    binary.LittleEndian.PutUint32(data[4:8], entry.CurrentTerm)
    binary.LittleEndian.PutUint32(data[8:12], entry.VotedFor)
    binary.LittleEndian.PutUint32(data[12:16], uint32(len(entry.Key)))
    binary.LittleEndian.PutUint32(data[16:20], uint32(len(entry.Value)))
    copy(data[20:], []byte(entry.Key))
    copy(data[20+len(entry.Key):], entry.Value)

    _, err = writer.Write(data)
    if err != nil {
        return 0, err
    }

    return offset, nil
}