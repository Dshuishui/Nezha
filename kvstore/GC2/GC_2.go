package GC2

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"container/heap"
	"time"
	// "math"
	// "sync"
	// "runtime"
	// "path/filepath"
)

const (
	// filePath     = "/path/to/your/file"  // 替换为您要监控的文件路径
	threshold     = 1024 * 1024 * 1024 * 10 // 10 GB, 根据需要调整
	checkInterval = 8 * time.Second         // 每8秒检查一次
	GCedPath      = "./kvstore/FlexSync/db_key_index_withGC"
	// batchSize     = 1000000 // Adjust this based on available memory
	targetBatchSizeBytes  = 3 * 1024 * 1024 * 1024 // 4GB in bytes
)

type Entry struct {
	Index       uint32
	CurrentTerm uint32
	VotedFor    uint32
	Key         string
	Value       string
}

type EntryHeapItem struct {
	entry  *Entry
	reader *bufio.Reader
	index  int
}

type EntryHeap []*EntryHeapItem

func (h EntryHeap) Len() int           { return len(h) }
func (h EntryHeap) Less(i, j int) bool { return h[i].entry.Key < h[j].entry.Key }	// 说明是从小到大排序，是一个最小堆
func (h EntryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *EntryHeap) Push(x interface{}) {
	*h = append(*h, x.(*EntryHeapItem))
}

func (h *EntryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

func mergeFiles(tempFiles []string, outputFilename string) error {
	outputFile, err := os.Create(outputFilename)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	h := &EntryHeap{}
	heap.Init(h)	// 创建堆结构体
	
	// 开启，多个临时文件以堆作为数据结构的排序。
	// Open all temp files and read the first entry from each
	for i, filename := range tempFiles {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer file.Close()

		reader := bufio.NewReader(file)
		entry, err := readEntry(reader)		// 读取文件的第一个entry
		if err != nil && err != io.EOF {	// 读取有错误，但是不是因读到文件的末尾而报错
			return err
		}
		if err != io.EOF {		// 将每个文件的第一个entry push到堆结构体中
			heap.Push(h, &EntryHeapItem{entry: entry, reader: reader, index: i})
		}
	}

	// 多个文件依次取出entry进行堆排序。
	// Merge entries
	for h.Len() > 0 {
		item := heap.Pop(h).(*EntryHeapItem)
		err := writeEntry(writer, item.entry)	// 取出堆顶entry 并写入新的磁盘文件
		if err != nil {
			return err
		}

		// Read next entry from the same file
		nextEntry, err := readEntry(item.reader)	// 取走了该文件的一个entry，需要再读取该文件的下一个entry压入堆中
		if err == nil {
			heap.Push(h, &EntryHeapItem{entry: nextEntry, reader: item.reader, index: item.index})
		} else if err != io.EOF {
			return err
		}
	}

	return nil
}

func approximateMapSize(m map[string]*Entry) int {
	size := 0
	for k, v := range m {
		// 估算每个 key 的大小
		size += len(k)
		// 估算每个 Entry 的大小
		size += 4 * 3 // 3 个 uint32 字段
		size += len(v.Key) + len(v.Value)
	}
	return size
}

func processLargeFile(inputFilename, outputFilename string) error {
	inputFile, err := os.Open(inputFilename)
	if err != nil {
		return fmt.Errorf("error opening input file: %v", err)
	}
	defer inputFile.Close()

	reader := bufio.NewReader(inputFile)

	tempFiles := []string{}
	entryMap := make(map[string]*Entry)
	// count := 0

	for {
		entry, err := readEntry(reader)	  // 读取一个entry
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading entry: %v", err)
		}

		entryMap[entry.Key] = entry
		// count++

		// 计算 batchSize
		// batchSize := int(math.Min(float64(maxBatchSizeBytes/averageEntrySize), math.MaxInt32))

		// if count >= batchSize {
		// 这个计算方式可能有问题。映射的实际内存占用通常比存储的元素数量要大，因为它需要额外的空间来支持高效的哈希操作。
		if approximateMapSize(entryMap) > targetBatchSizeBytes {
			tempFile, err := writeTempFile(entryMap)  // 取一批entry实体放入临时文件进行排序。
			if err != nil {
				return fmt.Errorf("error writing temp file: %v", err)
			}
			tempFiles = append(tempFiles, tempFile)  // 记录所有临时文件的路径
			entryMap = make(map[string]*Entry)		// 初始化进行下一个批次
			// count = 0

		}
	}

	if len(entryMap) > 0 {		// 说明读完了整个文件，还有一个不满足一批次的部分entry实体，接下来将剩余的部分进行排序。
		tempFile, err := writeTempFile(entryMap)	
		if err != nil {
			return fmt.Errorf("error writing final temp file: %v", err)
		}
		tempFiles = append(tempFiles, tempFile)		
	}

	err = mergeFiles(tempFiles, outputFilename)
	if err != nil {
		return fmt.Errorf("error merging files: %v", err)
	}

	// Clean up temp files
	for _, file := range tempFiles {
		os.Remove(file)
	}

	return nil
}

func readEntry(reader *bufio.Reader) (*Entry, error) {
	entry := &Entry{}
	
	err := binary.Read(reader, binary.LittleEndian, &entry.Index)
	if err != nil {
		return nil, fmt.Errorf("读取Index错误: %v", err)
	}

	err = binary.Read(reader, binary.LittleEndian, &entry.CurrentTerm)
	if err != nil {
		return nil, fmt.Errorf("读取CurrentTerm错误: %v", err)
	}

	err = binary.Read(reader, binary.LittleEndian, &entry.VotedFor)
	if err != nil {
		return nil, fmt.Errorf("读取VotedFor错误: %v", err)
	}

	var keySize, valueSize uint32
	err = binary.Read(reader, binary.LittleEndian, &keySize)
	if err != nil {
		return nil, fmt.Errorf("读取keySize错误: %v", err)
	}

	err = binary.Read(reader, binary.LittleEndian, &valueSize)
	if err != nil {
		return nil, fmt.Errorf("读取valueSize错误: %v", err)
	}

	keyBytes := make([]byte, keySize)
	_, err = io.ReadFull(reader, keyBytes)
	if err != nil {
		return nil, fmt.Errorf("读取key错误: %v", err)
	}
	entry.Key = string(keyBytes)

	valueBytes := make([]byte, valueSize)
	_, err = io.ReadFull(reader, valueBytes)
	if err != nil {
		return nil, fmt.Errorf("读取value错误: %v", err)
	}
	entry.Value = string(valueBytes)

	return entry, nil
}

func writeTempFile(entryMap map[string]*Entry) (string, error) {
	tempFile, err := os.CreateTemp("kvstore/FlexSync", "entries_*.tmp")
	if err != nil {
		return "", err
	}
	defer tempFile.Close()

	writer := bufio.NewWriter(tempFile)
	defer writer.Flush()

	entries := make([]*Entry, 0, len(entryMap))
	for _, entry := range entryMap {
		entries = append(entries, entry)
	}

	sort.Slice(entries, func(i, j int) bool {	// 直接根据entry中的key进行排序
		return entries[i].Key < entries[j].Key
	})

	for _, entry := range entries {
		err := writeEntry(writer, entry)
		if err != nil {
			return "", err
		}
	}

	return tempFile.Name(), nil
}

func writeEntry(writer *bufio.Writer, entry *Entry) error {
	err := binary.Write(writer, binary.LittleEndian, entry.Index)
	if err != nil {
		return fmt.Errorf("写入Index错误: %v", err)
	}

	err = binary.Write(writer, binary.LittleEndian, entry.CurrentTerm)
	if err != nil {
		return fmt.Errorf("写入CurrentTerm错误: %v", err)
	}

	err = binary.Write(writer, binary.LittleEndian, entry.VotedFor)
	if err != nil {
		return fmt.Errorf("写入VotedFor错误: %v", err)
	}

	err = binary.Write(writer, binary.LittleEndian, uint32(len(entry.Key)))
	if err != nil {
		return fmt.Errorf("写入keySize错误: %v", err)
	}

	err = binary.Write(writer, binary.LittleEndian, uint32(len(entry.Value)))
	if err != nil {
		return fmt.Errorf("写入valueSize错误: %v", err)
	}

	_, err = writer.WriteString(entry.Key)
	if err != nil {
		return fmt.Errorf("写入key错误: %v", err)
	}

	_, err = writer.WriteString(entry.Value)
	if err != nil {
		return fmt.Errorf("写入value错误: %v", err)
	}

	return nil
}

// func mergeFiles(tempFiles []string, outputFilename string) error {
// 	outputFile, err := os.Create(outputFilename)
// 	if err != nil {
// 		return err
// 	}
// 	defer outputFile.Close()

// 	writer := bufio.NewWriter(outputFile)
// 	defer writer.Flush()

// 	readers := make([]*bufio.Reader, len(tempFiles))
// 	entries := make([]*Entry, len(tempFiles))

// 	for i, filename := range tempFiles {
// 		file, err := os.Open(filename)
// 		if err != nil {
// 			return err
// 		}
// 		defer file.Close()
// 		readers[i] = bufio.NewReader(file)		// 当前临时文件名的句柄
// 		entries[i], err = readEntry(readers[i])		// 当前临时文件中第一个entry，即该临时文件中，最小的key对应的entry
// 		if err != nil && err != io.EOF {
// 			return err
// 		}
// 	}

// 	for {
// 		minIndex := -1
// 		var minEntry *Entry

// 		for i, entry := range entries {		// 找到最小的key对应的entry实体以及其在entries中的index
// 			if entry != nil && (minEntry == nil || entry.Key < minEntry.Key) {
// 				minIndex = i
// 				minEntry = entry
// 			}
// 		}

// 		if minIndex == -1 {		// 说明entries里面为nil，直接退出
// 			break
// 		}

// 		err := writeEntry(writer, minEntry)		// 将最小的entry写入输出的文件
// 		if err != nil {
// 			return err
// 		}

// 		entries[minIndex], err = readEntry(readers[minIndex])
// 		if err != nil && err != io.EOF {
// 			return err
// 		}
// 	}

// 	return nil
// }


func handleGC(inputFilename string, outputFilename string) {
	err := processLargeFile(inputFilename, outputFilename)
	if err != nil {
		fmt.Printf("Error processing file: %v\n", err)
		return
	}

	fmt.Println("File processed successfully.")
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

// func processLargeFileParallel(inputFilename, outputFilename string, batchSize int) error {
// 	inputFile, err := os.Open(inputFilename)
// 	if err != nil {
// 		return fmt.Errorf("error opening input file: %v", err)
// 	}
// 	defer inputFile.Close()

// 	fileInfo, err := inputFile.Stat()
// 	if err != nil {
// 		return fmt.Errorf("error getting file info: %v", err)
// 	}

// 	// Calculate number of goroutines based on available CPU cores
// 	numWorkers := runtime.NumCPU()
// 	chunkSize := fileInfo.Size() / int64(numWorkers)
// 	if chunkSize < int64(batchSize) {
// 		numWorkers = int(fileInfo.Size() / int64(batchSize))
// 		if numWorkers == 0 {
// 			numWorkers = 1
// 		}
// 		chunkSize = fileInfo.Size() / int64(numWorkers)
// 	}

// 	var wg sync.WaitGroup
// 	tempFiles := make([]string, numWorkers)
// 	errChan := make(chan error, numWorkers)

// 	for i := 0; i < numWorkers; i++ {
// 		wg.Add(1)
// 		go func(workerID int) {
// 			defer wg.Done()

// 			startOffset := int64(workerID) * chunkSize
// 			endOffset := startOffset + chunkSize
// 			if workerID == numWorkers-1 {
// 				endOffset = fileInfo.Size()
// 			}

// 			tempFile, err := processFileChunk(inputFilename, startOffset, endOffset, batchSize)
// 			if err != nil {
// 				errChan <- fmt.Errorf("worker %d error: %v", workerID, err)
// 				return
// 			}

// 			tempFiles[workerID] = tempFile
// 		}(i)
// 	}

// 	wg.Wait()
// 	close(errChan)

// 	for err := range errChan {
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	// Merge the temp files
// 	err = mergeFiles(tempFiles, outputFilename)
// 	if err != nil {
// 		return fmt.Errorf("error merging files: %v", err)
// 	}

// 	// Clean up temp files
// 	for _, file := range tempFiles {
// 		os.Remove(file)
// 	}

// 	return nil
// }

// func processFileChunk(filename string, startOffset, endOffset int64, batchSize int) (string, error) {
// 	file, err := os.Open(filename)
// 	if err != nil {
// 		return "", err
// 	}
// 	defer file.Close()

// 	_, err = file.Seek(startOffset, io.SeekStart)
// 	if err != nil {
// 		return "", err
// 	}

// 	reader := bufio.NewReader(file)
// 	entryMap := make(map[string]*Entry)
// 	count := 0

// 	tempFile, err := os.CreateTemp("", "entries_*.tmp")
// 	if err != nil {
// 		return "", err
// 	}
// 	defer tempFile.Close()

// 	writer := bufio.NewWriter(tempFile)
// 	defer writer.Flush()

// 	for {
// 		entry, err := readEntry(reader)
// 		if err == io.EOF || (count > 0 && file.Seek(0, io.SeekCurrent) >= endOffset) {
// 			break
// 		}
// 		if err != nil {
// 			return "", err
// 		}

// 		entryMap[entry.Key] = entry
// 		count++

// 		if count >= batchSize {
// 			err = writeEntriesInOrder(writer, entryMap)
// 			if err != nil {
// 				return "", err
// 			}
// 			entryMap = make(map[string]*Entry)
// 			count = 0
// 		}
// 	}

// 	if len(entryMap) > 0 {
// 		err = writeEntriesInOrder(writer, entryMap)
// 		if err != nil {
// 			return "", err
// 		}
// 	}

// 	return tempFile.Name(), nil
// }

// func writeEntriesInOrder(writer *bufio.Writer, entryMap map[string]*Entry) error {
// 	keys := make([]string, 0, len(entryMap))
// 	for k := range entryMap {
// 		keys = append(keys, k)
// 	}
// 	sort.Strings(keys)

// 	for _, key := range keys {
// 		err := writeEntry(writer, entryMap[key])
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }