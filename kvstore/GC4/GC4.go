// go get github.com/hashicorp/golang-lru

import (
    "bufio"
    "encoding/binary"
    "fmt"
    "os"
    "sort"
    "time"

    lru "github.com/hashicorp/golang-lru"
)

// Entry 结构体定义（如果在其他地方已定义，可以删除这部分）
type Entry struct {
    Index       uint32
    CurrentTerm uint32
    VotedFor    uint32
    Key         string
    Value       string
}

func (kvs *KVServer) GarbageCollection() error {
    fmt.Println("Starting garbage collection...")
    startTime := time.Now()

    // 创建LRU缓存
    // 假设我们允许缓存占用 100MB 内存，每个条目占 20B
    // 100MB / 20B = 5,000,000 个条目
    cache, _ := lru.New(5000000)

    // 打开原始文件
    file, err := os.Open("./raft/RaftState.log")
    if err != nil {
        return fmt.Errorf("failed to open file: %v", err)
    }
    defer file.Close()

    // 创建一个map来存储最新的entries
    latestEntries := make(map[string]*Entry)

    // 读取文件并处理entries
    reader := bufio.NewReader(file)
    var currentOffset int64 = 0
    for {
        entry, entryOffset, err := readEntry(reader, currentOffset)
        if err != nil {
            if err.Error() == "EOF" {
                break
            }
            return fmt.Errorf("error reading entry: %v", err)
        }

        // 更新当前偏移量
        currentOffset = entryOffset + int64(binary.Size(entry.Index) + binary.Size(entry.CurrentTerm) + 
                        binary.Size(entry.VotedFor) + 8 + len(entry.Key) + len(entry.Value))

        // 验证entry是否有效
        if isValidEntry(kvs, entry, entryOffset, cache) {
            latestEntries[entry.Key] = entry
        }
    }

    // 将map转换为slice并排序
    sortedEntries := make([]*Entry, 0, len(latestEntries))
    for _, entry := range latestEntries {
        sortedEntries = append(sortedEntries, entry)
    }
    sort.Slice(sortedEntries, func(i, j int) bool {
        return sortedEntries[i].Key < sortedEntries[j].Key
    })

    // 写入新文件
    // err = writeEntriesToNewFile(sortedEntries)
	// 写入新的排序文件
    newFilePath := "./raft/RaftState_sorted.log"
    err = writeEntriesToNewFile(sortedEntries, newFilePath)
    if err != nil {
        return fmt.Errorf("failed to write new file: %v", err)
    }

    fmt.Printf("Garbage collection completed in %v\n", time.Since(startTime))
    return nil
}

func readEntry(reader *bufio.Reader, currentOffset int64) (*Entry, int64, error) {
    var entry Entry
    var keySize, valueSize uint32

    entryStartOffset := currentOffset

    // 读取固定大小的字段
    if err := binary.Read(reader, binary.LittleEndian, &entry.Index); err != nil {
        return nil, 0, err
    }
    currentOffset += 4 // uint32 大小

    if err := binary.Read(reader, binary.LittleEndian, &entry.CurrentTerm); err != nil {
        return nil, 0, err
    }
    currentOffset += 4

    if err := binary.Read(reader, binary.LittleEndian, &entry.VotedFor); err != nil {
        return nil, 0, err
    }
    currentOffset += 4

    if err := binary.Read(reader, binary.LittleEndian, &keySize); err != nil {
        return nil, 0, err
    }
    currentOffset += 4

    if err := binary.Read(reader, binary.LittleEndian, &valueSize); err != nil {
        return nil, 0, err
    }
    currentOffset += 4

    // 读取key和value
    keyBytes := make([]byte, keySize)
    if _, err := reader.Read(keyBytes); err != nil {
        return nil, 0, err
    }
    entry.Key = string(keyBytes)
    currentOffset += int64(keySize)

    valueBytes := make([]byte, valueSize)
    if _, err := reader.Read(valueBytes); err != nil {
        return nil, 0, err
    }
    entry.Value = string(valueBytes)
    currentOffset += int64(valueSize)

    return &entry, entryStartOffset, nil
}

func IsValidEntry(kvs *KVServer, entry *Entry, entryOffset int64, cache *lru.Cache) bool {
    // 检查缓存中是否已有该key的偏移量
    if cachedOffset, ok := cache.Get(entry.Key); ok {
        return cachedOffset.(int64) == entryOffset
    }

    // 如果缓存中没有，从RocksDB中获取
    position, err := kvs.persister.Get_opt(entry.Key)
    if err != nil {
        return false
    }

    // 将结果加入缓存
    cache.Add(entry.Key, position)

    // 比较偏移量
    return position == entryOffset
}

func writeEntriesToNewFile(entries []*Entry, string newFilePath) error {
    file, err := os.Create(newFilePath)		// 后续可能需要换成动态的更改文件名，动态在后续加一个_GC
    if err != nil {
        return err
    }
    defer file.Close()

    writer := bufio.NewWriter(file)
    for _, entry := range entries {
        if err := writeEntry(writer, entry); err != nil {
            return err
        }
    }

    return writer.Flush()
}

func writeEntry(writer *bufio.Writer, entry *Entry) error {
    if err := binary.Write(writer, binary.LittleEndian, entry.Index); err != nil {
        return err
    }
    if err := binary.Write(writer, binary.LittleEndian, entry.CurrentTerm); err != nil {
        return err
    }
    if err := binary.Write(writer, binary.LittleEndian, entry.VotedFor); err != nil {
        return err
    }

    keySize := uint32(len(entry.Key))
    valueSize := uint32(len(entry.Value))

    if err := binary.Write(writer, binary.LittleEndian, keySize); err != nil {
        return err
    }
    if err := binary.Write(writer, binary.LittleEndian, valueSize); err != nil {
        return err
    }

    if _, err := writer.WriteString(entry.Key); err != nil {
        return err
    }
    if _, err := writer.WriteString(entry.Value); err != nil {
        return err
    }

    return nil
}