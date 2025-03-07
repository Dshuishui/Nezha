package raft

import (
	// "fmt"
	"os"
	"sync"
	"time"
)

// FilePool 是单个文件的描述符池
type FilePool struct {
    mu       sync.Mutex
    file     *os.File
    filePath string
    isOpen   bool
    lastUsed time.Time
}

// FilePoolManager 管理多个文件的描述符池
type FilePoolManager struct {
    mu        sync.Mutex
    pools     map[string]*FilePool
    maxPools  int           // 最大池数量
    idleTime  time.Duration // 空闲时间阈值
}

// NewFilePoolManager 创建一个新的文件池管理器
func NewFilePoolManager(maxPools int, idleTime time.Duration) *FilePoolManager {
    manager := &FilePoolManager{
        pools:    make(map[string]*FilePool),
        maxPools: maxPools,
        idleTime: idleTime,
    }
    
    // 启动定期清理协程
    go manager.cleanupLoop()
    
    return manager
}

// cleanupLoop 定期检查并关闭空闲文件
func (fpm *FilePoolManager) cleanupLoop() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    
    for range ticker.C {
        fpm.mu.Lock()
        now := time.Now()
        for _, pool := range fpm.pools {
            pool.mu.Lock()
            if pool.isOpen && now.Sub(pool.lastUsed) > fpm.idleTime {
                if pool.file != nil {
                    pool.file.Close()
                    pool.file = nil
                    pool.isOpen = false
                }
            }
            pool.mu.Unlock()
        }
        fpm.mu.Unlock()
    }
}

// GetPool 获取指定路径的文件池
func (fpm *FilePoolManager) GetPool(filePath string) *FilePool {
    fpm.mu.Lock()
    defer fpm.mu.Unlock()
    
    if pool, exists := fpm.pools[filePath]; exists {
        return pool
    }
    
    // 如果池数量达到上限，关闭最旧的池
    if len(fpm.pools) >= fpm.maxPools {
        var oldestPath string
        var oldestTime time.Time
        
        for path, pool := range fpm.pools {
            pool.mu.Lock()
            if oldestPath == "" || pool.lastUsed.Before(oldestTime) {
                oldestPath = path
                oldestTime = pool.lastUsed
            }
            pool.mu.Unlock()
        }
        
        if oldestPath != "" {
            oldPool := fpm.pools[oldestPath]
            oldPool.mu.Lock()
            if oldPool.isOpen && oldPool.file != nil {
                oldPool.file.Close()
            }
            oldPool.mu.Unlock()
            delete(fpm.pools, oldestPath)
        }
    }
    
    // 创建新池
    pool := &FilePool{
        filePath: filePath,
        isOpen:   false,
        lastUsed: time.Now(),
    }
    fpm.pools[filePath] = pool
    return pool
}

// ReleasePool 释放文件池的引用
func (fpm *FilePoolManager) ReleasePool(filePath string) {
    // 这里不做任何操作，由cleanupLoop定期清理
}

// Close 关闭所有文件池
func (fpm *FilePoolManager) Close() {
    fpm.mu.Lock()
    defer fpm.mu.Unlock()
    
    for _, pool := range fpm.pools {
        pool.mu.Lock()
        if pool.isOpen && pool.file != nil {
            pool.file.Close()
            pool.file = nil
            pool.isOpen = false
        }
        pool.mu.Unlock()
    }
}

// Get 获取文件描述符
func (fp *FilePool) Get() (*os.File, error) {
    fp.mu.Lock()
    defer fp.mu.Unlock()
    
    fp.lastUsed = time.Now()
    
    if !fp.isOpen || fp.file == nil {
        file, err := os.OpenFile(fp.filePath, os.O_RDWR|os.O_CREATE, 0666)
        if err != nil {
            return nil, err
        }
        fp.file = file
        fp.isOpen = true
    }
    
    return fp.file, nil
}

// Put 归还文件描述符
func (fp *FilePool) Put(file *os.File) {
    fp.mu.Lock()
    defer fp.mu.Unlock()
    
    fp.lastUsed = time.Now()
    // 不关闭文件，由清理协程处理
}