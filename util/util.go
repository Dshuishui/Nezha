package util

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"

	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.SetPrefix("[Debug] ")
		log.SetFlags(log.Ldate | log.Ltime)
		log.Printf(format, a...)
	}
	return
}

// 打印格式化后的错误信息
func EPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetPrefix("[Error] ")
	log.SetFlags(log.Ldate | log.Ltime)
	log.Printf(format, a...)
	return
}

// 打印格式化后的正常信息
func IPrintf(format string, a ...interface{}) (n int, err error) { // 空接口切片类型（...interface{}）可以接收任意类型的参数
	log.SetPrefix("[Info] ")            // 添加日志输出的前缀[Info]
	log.SetFlags(log.Ldate | log.Ltime) // 设置日志输出的格式化选项，log.Ldate表示在输出中包含日期，log.Ltime表示在输出中包含时间，用|结合两个选项，同时应用于日志输出。
	log.Printf(format, a...)            // 消息的格式由 format 提供，而后续的参数列表 a... 则是提供给格式化字符串的实际参数。
	return
}

// 打印格式化后监听失败的信息
func FPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetPrefix("[Fatalf] ")
	log.SetFlags(log.Ldate | log.Ltime)
	log.Printf(format, a...)
	return
}

/* csv读取 */
func ReadCsv(filepath string) (writeCounts []float64) {
	// 打开 CSV 文件
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// 创建 CSV 读取器
	reader := csv.NewReader(file)

	// 读取CSV数据
	rows, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	// 只处理前1000行
	if len(rows) > 1000 {
		rows = rows[:1000]
	}

	writeCounts = make([]float64, len(rows)-1)

	// 读取第二列数据，读的次数并存储在writeCounts数组中
	for j := 1; j < len(rows) && j <= 1000; j++ {
		writeCounts[j-1], err = strconv.ParseFloat(rows[j][1], 64)
		if err != nil {
			FPrintf(err.Error())
		}
	}
	return writeCounts
}

/* 数组写入csv */
func WriteCsv(filepath string, spentTimeArr []int) {
	file, err := os.Create(filepath) // 创建文件
	if err != nil {
		FPrintf(err.Error())
	}
	defer file.Close()

	writer := csv.NewWriter(file) // 创建一个新的 CSV writer，用于向文件中写入 CSV 格式的数据
	defer writer.Flush()          // 确保在函数返回之前将缓冲区中的数据刷新到文件中

	for _, value := range spentTimeArr {
		err := writer.Write([]string{strconv.Itoa(value)})
		if err != nil {
			FPrintf(err.Error())
		}
	}
}

// 记录每次执行put请求的时间
func Put_Request_Time(filePath string, executionTime time.Duration,key string ,value string,num_k int) error {
// Open the file in append mode, create it if not exists
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	// Create a new CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	var v string
	if len(value)<1024{
		v =strconv.Itoa(len(value))+"-B"
	}else{
		v =strconv.Itoa(len(value)/1024) + "-KB"
	}

	// Prepare the data to be appended
	data := []string{
		fmt.Sprintf("%s-%vB;%s;%d", key,len(key),v, num_k), // Concatenate key, value, and num
		fmt.Sprintf("%v", executionTime), // Calculate the time since the start time
	}

	// Write the data to the CSV file
	if err := writer.Write(data); err != nil {
		return fmt.Errorf("error writing to CSV: %v", err)
	}

	return nil
}

/*
sync.Map 相关的函数
*/
func Len(vc sync.Map) int {
	count := 0
	vc.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}
func BecomeMap(vc sync.Map) map[string]int32 {
	res := map[string]int32{}
	vc.Range(func(key, value any) bool {
		res[key.(string)] = value.(int32)
		return true
	})
	return res
}
func BecomeSyncMap(argMap map[string]int32) sync.Map {
	var res sync.Map
	for key, value := range argMap {
		res.Store(key, value)
	}
	return res
}

// 判断vectorClock是否更大（key都有，并且value>=other.value）
func IsUpper(vectorClock sync.Map, arg_vc sync.Map) bool {
	DPrintf("IsUpper(): vectorClock: %v, arg_vc: %v", BecomeMap(vectorClock), BecomeMap(arg_vc))
	if Len(arg_vc) == 0 {
		return true
	}
	if Len(vectorClock) < Len(arg_vc) {
		DPrintf("vectorClock's length is shorter")
		return false
	} else {
		res := true
		vectorClock.Range(func(k, v interface{}) bool {
			value, ok := arg_vc.Load(k)
			if ok {
				if v.(int32) >= value.(int32) {
					return true
				} else {
					res = false
					// 遍历终止
					return false
				}
			}
			// 传过来的客户端中的vectorClock中没有该key，这种情况只有在kvs增加了节点，客户端也不知道
			res = false
			// 遍历终止
			return false
		})
		return res
	}
}

func LoadInt(counts sync.Map, key string) int {
	// 检查代理记录中，有没有该key，如果有，则取出他的value,即该key的put次数
	// 如果没有，就返回0
	val, exist := counts.Load(key)
	if exist == false {
		return 0
	}
	return val.(int)
}

/* Map相关 */
func MakeMap(addresses []string) map[string]int32 {
	res := make(map[string]int32)
	for _, address := range addresses {
		res[address+"1"] = 0
	}
	return res
}

// 生成较大的value
func GenerateLargeValue(size int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	var buffer bytes.Buffer
	lettersLength := len(letters)
	for i := 0; i < size; i++ {
		randomLetter := letters[rand.Intn(lettersLength)]
		buffer.WriteByte(randomLetter)
	}
	return buffer.String()
}

// 生成size+3个字节大小的key，加3是因为前面还会将生成的字节数组与“key”拼在一起
func GenerateFixedSizeKey(size int) string {
	const letters = "0123456789"
	var buffer bytes.Buffer
	lettersLength := len(letters)
	for i := 0; i < size; i++ {
		randomLetter := letters[rand.Intn(lettersLength)]
		buffer.WriteByte(randomLetter)
	}
	return buffer.String()
}