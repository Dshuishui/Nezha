package gctime

import (
	"fmt"
	"sync"
	"time"
)

const (
	diskThreshold     = 80 // 80% of disk capacity
	idleTimeThreshold = 5 * time.Minute
)

type GarbageCollector struct {
	mutex               sync.Mutex
	isRunning           bool
	lastRequestTime     time.Time
	diskUsage           int
	gcInterval          time.Duration
	diskCheckInterval   time.Duration
	idleCheckInterval   time.Duration
	gcFunc              func() error
	stopChan            chan struct{}
	diskThresholdPercent int
	idleTimeThreshold    time.Duration
}

func NewGarbageCollector(gcFunc func() error) *GarbageCollector {
	return &GarbageCollector{
		lastRequestTime:     time.Now(),
		gcInterval:          30 * time.Minute,
		diskCheckInterval:   5 * time.Minute,
		idleCheckInterval:   1 * time.Minute,
		gcFunc:              gcFunc,
		stopChan:            make(chan struct{}),
		diskThresholdPercent: diskThreshold,
		idleTimeThreshold:    idleTimeThreshold,
	}
}

func (gc *GarbageCollector) Start() {
	go gc.run()
}

func (gc *GarbageCollector) Stop() {
	close(gc.stopChan)
}

func (gc *GarbageCollector) run() {
	gcTicker := time.NewTicker(gc.gcInterval)
	diskCheckTicker := time.NewTicker(gc.diskCheckInterval)
	idleCheckTicker := time.NewTicker(gc.idleCheckInterval)

	for {
		select {
		case <-gc.stopChan:
			gcTicker.Stop()
			diskCheckTicker.Stop()
			idleCheckTicker.Stop()
			return
		case <-gcTicker.C:
			gc.tryRunGC("regular interval")
		case <-diskCheckTicker.C:
			gc.checkDiskUsage()
		case <-idleCheckTicker.C:
			gc.checkIdleTime()
		}
	}
}

func (gc *GarbageCollector) checkDiskUsage() {
	// Simulating disk usage check
	// In a real system, you'd use OS-specific commands or libraries to get actual disk usage
	if gc.diskUsage > gc.diskThresholdPercent {
		gc.tryRunGC("disk usage threshold")
	}
}

func (gc *GarbageCollector) checkIdleTime() {
	gc.mutex.Lock()
	idleTime := time.Since(gc.lastRequestTime)
	gc.mutex.Unlock()

	if idleTime > gc.idleTimeThreshold {
		gc.tryRunGC("idle time threshold")
	}
}

func (gc *GarbageCollector) tryRunGC(reason string) {
	gc.mutex.Lock()
	defer gc.mutex.Unlock()

	if gc.isRunning {
		fmt.Printf("GC already running, skipping trigger: %s\n", reason)
		return
	}

	gc.isRunning = true
	go func() {
		fmt.Printf("Starting GC: %s\n", reason)
		err := gc.gcFunc()
		if err != nil {
			fmt.Printf("GC error: %v\n", err)
		} else {
			fmt.Println("GC completed successfully")
		}

		gc.mutex.Lock()
		gc.isRunning = false
		gc.mutex.Unlock()
	}()
}

func (gc *GarbageCollector) RecordClientRequest() {
	gc.mutex.Lock()
	defer gc.mutex.Unlock()
	gc.lastRequestTime = time.Now()
}

func (gc *GarbageCollector) UpdateDiskUsage(usage int) {
	gc.mutex.Lock()
	defer gc.mutex.Unlock()
	gc.diskUsage = usage
}

// Simulating garbage collection
func simulateGC() error {
	time.Sleep(3 * time.Second) // Simulating some work
	return nil
}

func main() {
	gc := NewGarbageCollector(simulateGC)
	gc.Start()
	defer gc.Stop()

	// Simulate system activity
	for i := 0; i < 10; i++ {
		gc.RecordClientRequest()
		gc.UpdateDiskUsage(70 + i*2) // Gradually increase disk usage
		time.Sleep(1 * time.Minute)
	}

	// Wait for a while to let GC potentially trigger
	time.Sleep(10 * time.Minute)
}