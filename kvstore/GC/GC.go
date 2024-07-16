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
	
	// 读取固定长度的头部（20字节）
	header := make([]byte, 20)
	_, err := io.ReadFull(file, header)   	// 读取文件的位置指针会往后移
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("读取头部错误: %v", err)
	}
	
	// 解析头部，使用小端
	entry.Index = binary.LittleEndian.Uint32(header[0:4])
	entry.CurrentTerm = binary.LittleEndian.Uint32(header[4:8])
	entry.VotedFor = binary.LittleEndian.Uint32(header[8:12])
	keySize := binary.LittleEndian.Uint32(header[12:16])
	valueSize := binary.LittleEndian.Uint32(header[16:20])
	
	// 读取 key，虽然名字涉及到扩充，但是只是普通的key
	paddedKey := make([]byte, keySize)
	_, err = io.ReadFull(file, paddedKey)
	if err != nil {
		return nil, fmt.Errorf("读取key错误: %v", err)
	}
	entry.Key = string(paddedKey)
	
	// 读取 value
	value := make([]byte, valueSize)
	_, err = io.ReadFull(file, value)
	if err != nil {
		return nil, fmt.Errorf("读取value错误: %v", err)
	}
	entry.Value = string(value)
	
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
	paddedKey := entry.Key   	// 虽然名字涉及到扩充，但是只是普通的key
	keySize := uint32(len(paddedKey))
	valueSize := uint32(len(entry.Value))
	
	// 写入头部，使用小端
	if err := binary.Write(file, binary.LittleEndian, entry.Index); err != nil {
		return fmt.Errorf("写入Index错误: %v", err)
	}
	if err := binary.Write(file, binary.LittleEndian, entry.CurrentTerm); err != nil {
		return fmt.Errorf("写入CurrentTerm错误: %v", err)
	}
	if err := binary.Write(file, binary.LittleEndian, entry.VotedFor); err != nil {
		return fmt.Errorf("写入VotedFor错误: %v", err)
	}
	if err := binary.Write(file, binary.LittleEndian, keySize); err != nil {
		return fmt.Errorf("写入keySize错误: %v", err)
	}
	if err := binary.Write(file, binary.LittleEndian, valueSize); err != nil {
		return fmt.Errorf("写入valueSize错误: %v", err)
	}
	
	// 写入key和value
	if _, err := file.Write([]byte(paddedKey)); err != nil {
		return fmt.Errorf("写入key错误: %v", err)
	}
	if _, err := file.Write([]byte(entry.Value)); err != nil {
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
			fmt.Println("读取entry时遇到EOF了")
			break
		}
		if err != nil {
			return fmt.Errorf("读取Entry错误: %v", err)
		}
		entries[entry.Key] = entry		// 构造一个map映射，方便后续的排序和根据key直接拿取到对应的entry实体。
	}
	fmt.Printf("读取出的entrys的长度为%v\n",len(entries))
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
			return
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
