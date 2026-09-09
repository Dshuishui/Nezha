package kvstore

import (
	"fmt"
	"os"
	"sync"
	"syscall"
)

type FileDescriptorPool struct {
	filePath string        // 文件路径
	pool     chan *os.File // 文件描述符池
	// seed 是创建时打开的那个描述符，**从不交出去**，只用来在池空时 dup 出新的。
	// 见 Get 里的说明：按路径重开会在文件被删之后失败。
	seed   *os.File
	mu     sync.Mutex // 保护池的互斥锁
	closed bool
}

// NewFileDescriptorPool 创建一个文件描述符池。
//
// 池是**惰性**的：创建时只打开一个描述符，其余按需打开、用完归还，poolSize 只是归还上限。
// 改造前是创建时一次性打开 poolSize 个——单个排序文件时无所谓，分区化之后这个数要乘以分区数：
// 128MB 分区、10GB 数据是 80 个分区，按原来的 50 就要 4000 个描述符，直接撞上默认 1024 的
// ulimit。惰性之后稳态描述符数由实际读并发决定，与分区数无关。
//
// 仍然在创建时打开一个，是为了让"文件不存在或打不开"在这里就报错，而不是拖到第一次读。
func NewFileDescriptorPool(filePath string, poolSize int) (*FileDescriptorPool, error) {
	if poolSize <= 0 {
		poolSize = 1
	}
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	seed, err := os.Open(filePath)
	if err != nil {
		file.Close()
		return nil, err
	}
	pool := make(chan *os.File, poolSize)
	pool <- file
	return &FileDescriptorPool{
		filePath: filePath,
		pool:     pool,
		seed:     seed,
	}, nil
}

// Get 从池中获取一个文件描述符
func (p *FileDescriptorPool) Get() (*os.File, error) {
	select {
	case file := <-p.pool:
		if file != nil {
			return file, nil
		}
		// 池已关闭：从已关闭的通道取到的是零值，不能当描述符用，退回去自己打开
	default:
	}
	// 池空了，从 seed **dup** 一个而不是按路径重开。
	//
	// 按路径重开会在文件已被删除时失败，而 GC 收尾正好会删掉被取代的分区文件：
	// finishAnotherGC 在释放锁之后才删，此时可能仍有读者持着上一组分区。读路径用
	// mmap，已打开的描述符在 unlink 之后照样能读，唯独"池空 → 按路径重开"这一步会
	// 撞上已删除的路径。dup 出来的描述符指向同一个 inode，与路径是否还在无关。
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.seed == nil {
		return nil, fmt.Errorf("descriptor pool for %s is closed", p.filePath)
	}
	fd, err := syscall.Dup(int(p.seed.Fd()))
	if err != nil {
		return nil, fmt.Errorf("dup descriptor for %s: %v", p.filePath, err)
	}
	return os.NewFile(uintptr(fd), p.filePath), nil
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

// Close 关闭池中的所有文件描述符。可重复调用。
func (p *FileDescriptorPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	close(p.pool)
	for file := range p.pool {
		file.Close()
	}
	if p.seed != nil {
		p.seed.Close()
		p.seed = nil
	}
}
