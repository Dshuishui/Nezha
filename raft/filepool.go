package raft

import (
	"os"
	"sync"
)

// FilePool 是一个文件描述符池，用于管理和重用文件描述符
type FilePool struct {
	mu      sync.Mutex
	path    string
	files   []*os.File
	inUse   map[*os.File]bool
	maxSize int
}

// NewFilePool 创建一个新的文件描述符池
func NewFilePool(path string, maxSize int) (*FilePool, error) {
	if maxSize <= 0 {
		maxSize = 10 // 默认池大小
	}
	
	return &FilePool{
		path:    path,
		files:   make([]*os.File, 0, maxSize),
		inUse:   make(map[*os.File]bool),
		maxSize: maxSize,
	}, nil
}

// Get 从池中获取一个文件描述符，如果池中没有可用的，则创建一个新的
func (fp *FilePool) Get() (*os.File, error) {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	
	// 查找未使用的文件描述符
	for _, file := range fp.files {
		if !fp.inUse[file] {
			fp.inUse[file] = true
			return file, nil
		}
	}
	
	// 如果没有可用的文件描述符，且池未满，则创建一个新的
	if len(fp.files) < fp.maxSize {
		file, err := os.Open(fp.path)
		if err != nil {
			return nil, err
		}
		fp.files = append(fp.files, file)
		fp.inUse[file] = true
		return file, nil
	}
	
	// 如果池已满，则等待一个文件描述符变为可用
	// 在实际应用中，可能需要实现更复杂的等待机制
	return nil, nil
}

// Put 将文件描述符归还到池中
func (fp *FilePool) Put(file *os.File) {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	
	if _, exists := fp.inUse[file]; exists {
		fp.inUse[file] = false
	}
}

// Close 关闭池中的所有文件描述符
func (fp *FilePool) Close() {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	
	for _, file := range fp.files {
		file.Close()
	}
	fp.files = nil
	fp.inUse = make(map[*os.File]bool)
}