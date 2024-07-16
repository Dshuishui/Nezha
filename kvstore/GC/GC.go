package GC

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	// "strings"
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

func readEntry(file *os.File) (*Entry, error) {
	entry := &Entry{}
	
	err := binary.Read(file, binary.LittleEndian, &entry.Index)
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("读取Index错误: %v", err)
	}
	// Read函数会根据第三个参数的类型来确定读取的字节数。
	// 会读取足够多的字节以填充entry.Index字段，
	// 如果读取的字节不足以构成一个完整的Index字段，
	// 或者遇到文件结束符io.EOF，binary.Read将返回相应的错误或者结束信号。
	err = binary.Read(file, binary.LittleEndian, &entry.CurrentTerm)
	if err != nil {
		return nil, fmt.Errorf("读取CurrentTerm错误: %v", err)
	}
	
	err = binary.Read(file, binary.LittleEndian, &entry.VotedFor)
	if err != nil {
		return nil, fmt.Errorf("读取VotedFor错误: %v", err)
	}
	
	var keySize, valueSize uint32
	err = binary.Read(file, binary.LittleEndian, &keySize)
	if err != nil {
		return nil, fmt.Errorf("读取keySize错误: %v", err)
	}
	
	err = binary.Read(file, binary.LittleEndian, &valueSize)
	if err != nil {
		return nil, fmt.Errorf("读取valueSize错误: %v", err)
	}
	
	keyBytes := make([]byte, keySize)
	_, err = io.ReadFull(file, keyBytes)
	if err != nil {
		return nil, fmt.Errorf("读取key错误: %v", err)
	}
	entry.Key = string(keyBytes)
	
	valueBytes := make([]byte, valueSize)
	_, err = io.ReadFull(file, valueBytes)
	if err != nil {
		return nil, fmt.Errorf("读取value错误: %v", err)
	}
	entry.Value = string(valueBytes)
	
	return entry, nil
}

// func deduplicateEntries(entries []Entry) map[string]Entry {
// 	entryMap := make(map[string]Entry)
// 	for _, entry := range entries {
// 		entryMap[entry.Key] = entry
// 	}
// 	return entryMap
// }

// func sortEntries(entryMap map[string]Entry) []Entry {	// 直接根据rntry中key进行排序。
// 	var sortedEntries []Entry
// 	for _, entry := range entryMap {
// 		sortedEntries = append(sortedEntries, entry)
// 	}
// 	sort.Slice(sortedEntries, func(i, j int) bool {
// 		return strings.Compare(sortedEntries[i].Key, sortedEntries[j].Key) < 0
// 	})
// 	return sortedEntries
// }

func writeEntry(file *os.File, entry *Entry) error {
	err := binary.Write(file, binary.LittleEndian, entry.Index)
	if err != nil {
		return fmt.Errorf("写入Index错误: %v", err)
	}
	
	err = binary.Write(file, binary.LittleEndian, entry.CurrentTerm)
	if err != nil {
		return fmt.Errorf("写入CurrentTerm错误: %v", err)
	}
	
	err = binary.Write(file, binary.LittleEndian, entry.VotedFor)
	if err != nil {
		return fmt.Errorf("写入VotedFor错误: %v", err)
	}
	
	err = binary.Write(file, binary.LittleEndian, uint32(len(entry.Key)))
	if err != nil {
		return fmt.Errorf("写入keySize错误: %v", err)
	}
	
	err = binary.Write(file, binary.LittleEndian, uint32(len(entry.Value)))
	if err != nil {
		return fmt.Errorf("写入valueSize错误: %v", err)
	}
	
	_, err = file.Write([]byte(entry.Key))
	if err != nil {
		return fmt.Errorf("写入key错误: %v", err)
	}
	
	_, err = file.Write([]byte(entry.Value))
	if err != nil {
		return fmt.Errorf("写入value错误: %v", err)
	}
	
	return nil
}

func garbageCollect(inputFilename string, outputFilename string) error{
	fmt.Println("Garbage collection started")
	inputFile, err := os.Open(inputFilename)
	if err != nil {
		return fmt.Errorf("打开输入文件错误: %v", err)
	}
	defer inputFile.Close()
	
	entries := make(map[string]*Entry)
	
	for {
		entry, err := readEntry(inputFile)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("读取Entry错误: %v", err)
		}
		entries[entry.Key] = entry		// 构造一个map映射，方便后续的排序和根据key直接拿取到对应的entry实体。
	}
	sortedKeys := make([]string, 0, len(entries))
	for key := range entries {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)		// 得到所有entry中key的一个排序
	
	outputFile, err := os.Create(outputFilename)
	if err != nil {
		return fmt.Errorf("创建输出文件错误: %v", err)
	}
	defer outputFile.Close()
	
	for _, key := range sortedKeys {
		err = writeEntry(outputFile, entries[key])	// 按照排好序的key，依次取出key对应的entry，同时把entry写入磁盘文件。
		if err != nil {
			return fmt.Errorf("写入Entry错误: %v", err)
		}
	}

	return nil
}

func MonitorFileSize(path string) {
	for {
		size, err := getFileSize(path)
		fmt.Printf("get File Size %v\n",size)
		if err != nil {
			fmt.Printf("Error checking file size: %v\n", err)
		} else if size > threshold {
			fmt.Println("Garbage collection starting.")
			err := garbageCollect(path, GCedPath)
			if err != nil {
				fmt.Printf("垃圾回收错误: %v\n", err)
				return
			}
			fmt.Println("Garbage collection completed successfully.")
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
