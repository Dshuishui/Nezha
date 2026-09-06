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
