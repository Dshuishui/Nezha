package GC

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	// filePath     = "/path/to/your/file"  // 替换为您要监控的文件路径
	threshold     = 1024 * 1024 * 1024 * 10 // 10 GB, 根据需要调整
	checkInterval = 8 * time.Second         // 每5秒检查一次
	GCedPath      = "./kvstore/FlexSync/db_key_index_withGC"
)

type Entry struct {
	Index       uint32
	CurrentTerm uint32
	VotedFor    uint32
	Key         string
	Value       string
}

func readEntries(filename string) ([]Entry, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []Entry

	for {
		entry := Entry{}
		// Read函数会根据第三个参数的类型来确定读取的字节数。
		// 会读取足够多的字节以填充entry.Index字段，
		// 如果读取的字节不足以构成一个完整的Index字段，
		// 或者遇到文件结束符io.EOF，binary.Read将返回相应的错误或者结束信号。
		err = binary.Read(file, binary.LittleEndian, &entry.Index)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		err = binary.Read(file, binary.LittleEndian, &entry.CurrentTerm)
		if err != nil {
			return nil, err
		}

		err = binary.Read(file, binary.LittleEndian, &entry.VotedFor)
		if err != nil {
			return nil, err
		}

		var keyLength uint32
		err = binary.Read(file, binary.LittleEndian, &keyLength)
		if err != nil {
			return nil, err
		}

		var valueLength uint32
		err = binary.Read(file, binary.LittleEndian, &valueLength)
		if err != nil {
			return nil, err
		}

		keyBytes := make([]byte, keyLength)
		_, err = io.ReadFull(file, keyBytes)
		if err != nil {
			return nil, err
		}
		entry.Key = string(keyBytes)

		valueBytes := make([]byte, valueLength)
		_, err = io.ReadFull(file, valueBytes)
		if err != nil {
			return nil, err
		}
		entry.Value = string(valueBytes)

		entries = append(entries, entry)
	}

	return entries, nil
}

func deduplicateEntries(entries []Entry) map[string]Entry {
	entryMap := make(map[string]Entry)
	for _, entry := range entries {
		entryMap[entry.Key] = entry
	}
	return entryMap
}

func sortEntries(entryMap map[string]Entry) []Entry {
	var sortedEntries []Entry
	for _, entry := range entryMap {
		sortedEntries = append(sortedEntries, entry)
	}
	sort.Slice(sortedEntries, func(i, j int) bool {
		return strings.Compare(sortedEntries[i].Key, sortedEntries[j].Key) < 0
	})
	return sortedEntries
}

func writeEntries(filename string, entries []Entry) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, entry := range entries {
		err = binary.Write(file, binary.LittleEndian, entry.Index)
		if err != nil {
			return err
		}

		err = binary.Write(file, binary.LittleEndian, entry.CurrentTerm)
		if err != nil {
			return err
		}

		err = binary.Write(file, binary.LittleEndian, entry.VotedFor)
		if err != nil {
			return err
		}

		err = binary.Write(file, binary.LittleEndian, uint32(len(entry.Key)))
		if err != nil {
			return err
		}

		err = binary.Write(file, binary.LittleEndian, uint32(len(entry.Value)))
		if err != nil {
			return err
		}

		_, err = file.Write([]byte(entry.Key))
		if err != nil {
			return err
		}

		_, err = file.Write([]byte(entry.Value))
		if err != nil {
			return err
		}
	}

	return nil
}

func handleGC(inputFilename string, outputFilename string) {
	entries, err := readEntries(inputFilename)
	if err != nil {
		fmt.Printf("Error reading entries: %v\n", err)
		return
	}

	deduplicatedEntries := deduplicateEntries(entries)
	sortedEntries := sortEntries(deduplicatedEntries)

	err = writeEntries(outputFilename, sortedEntries)
	if err != nil {
		fmt.Printf("Error writing entries: %v\n", err)
		return
	}

	fmt.Println("Garbage collection completed successfully.")
}

func MonitorFileSize(path string) {
	for {
		size, err := getFileSize(path)
		if err != nil {
			fmt.Printf("Error checking file size: %v\n", err)
		} else if size > threshold {
			handleGC(path, GCedPath)
		}
		time.Sleep(checkInterval)
	}
}

func getFileSize(path string) (int64, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}
