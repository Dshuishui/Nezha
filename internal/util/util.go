// Package util holds the logging helpers and the random key/value generators shared by
// the node and the benchmark tools.
package util

import (
	"bytes"
	"log"
	"math/rand"
)

// Debug enables DPrintf.
const Debug = true

// DPrintf logs a debug line (prefix "[Debug]") when Debug is set.
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.SetPrefix("[Debug] ")
		log.SetFlags(log.Ldate | log.Ltime)
		log.Printf(format, a...)
	}
	return
}

// EPrintf logs an error line (prefix "[Error]").
func EPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetPrefix("[Error] ")
	log.SetFlags(log.Ldate | log.Ltime)
	log.Printf(format, a...)
	return
}

// FPrintf logs a fatal-class line (prefix "[Fatalf]") without exiting.
// FPrintf 打印一条 [Fatalf] 前缀的日志。**它不退出进程**——前缀只是历史沿用的名字。
// 需要真的终止时用 log.Fatalf；把 FPrintf 当致命错误用过一次，代价是 listen 失败之后
// 带着 nil 监听器继续走、炸在 Serve 里（见 RegisterKVServer 的注释）。
func FPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetPrefix("[Fatalf] ")
	log.SetFlags(log.Ldate | log.Ltime)
	log.Printf(format, a...)
	return
}

// GenerateLargeValue returns a random lower-case string of size bytes.
func GenerateLargeValue(size int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	var buffer bytes.Buffer
	for i := 0; i < size; i++ {
		buffer.WriteByte(letters[rand.Intn(len(letters))])
	}
	return buffer.String()
}

// GenerateFixedSizeKey returns a random string of size non-zero digits; callers prefix it
// with "key", so the first digit being non-zero keeps numeric keys distinct.
func GenerateFixedSizeKey(size int) string {
	const nonZeroLetters = "123456789"
	var buffer bytes.Buffer
	for i := 0; i < size; i++ {
		buffer.WriteByte(nonZeroLetters[rand.Intn(len(nonZeroLetters))])
	}
	return buffer.String()
}
