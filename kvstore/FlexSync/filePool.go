package main

import (
	"os"
	"sync"
)

type FileDescriptorPool struct {
	filePath string       // 文件路径
	pool     chan *os.File // 文件描述符池
	mu       sync.Mutex   // 保护池的互斥锁
}

// NewFileDescriptorPool 创建一个文件描述符池
func NewFileDescriptorPool(filePath string, poolSize int) (*FileDescriptorPool, error) {
	pool := make(chan *os.File, poolSize)
	for i := 0; i < poolSize; i++ {
		file, err := os.Open(filePath)
		if err != nil {
			// 如果打开文件失败，关闭已经打开的文件并返回错误
			for f := range pool {
				f.Close()
			}
			return nil, err
		}
		pool <- file
	}
	return &FileDescriptorPool{
		filePath: filePath,
		pool:     pool,
	}, nil
}

// Get 从池中获取一个文件描述符
func (p *FileDescriptorPool) Get() (*os.File, error) {
	select {
	case file := <-p.pool:
		return file, nil
	default:
		// 如果池中没有可用的文件描述符，动态打开一个新的
		p.mu.Lock()
		defer p.mu.Unlock()
		file, err := os.Open(p.filePath)
		if err != nil {
			return nil, err
		}
		return file, nil
	}
}

// Put 将文件描述符归还到池中
func (p *FileDescriptorPool) Put(file *os.File) {
	select {
	case p.pool <- file:
		// 成功归还到池中
	default:
		// 如果池已满，关闭文件描述符
		file.Close()
	}
}

// Close 关闭池中的所有文件描述符
func (p *FileDescriptorPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	close(p.pool)
	for file := range p.pool {
		file.Close()
	}
}