package raft

import (
	"gitee.com/dong-shuishui/FlexSync/internal/util"

	// "github.com/syndtr/goleveldb/leveldb"
	// "github.com/syndtr/goleveldb/leveldb/errors"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/linxGnu/grocksdb"

	"sync"
	// "strconv"
)

// 存储层把 key 当作不透明的字节串：原样写入、原样返回，迭代顺序就是字节序。
// 这是 RocksDB / LevelDB / TiKV 的契约，**也是这里刚刚改成的契约**。
//
// 此前存储层会把每个 key 左补 '0' 到 KeyLength（曾是 10，后为 24）再存，取出时
// TrimLeft 掉前导零。那个编码只在"key 是十进制整数"这个定义域上成立：
//
//   - "7" 与 "007" 补齐后是同一个串，原始长度在写入时就丢了，后写的静默覆盖先写的；
//   - 超过 KeyLength 的 key 被**截断**，前 KeyLength 个字符相同的 key 全部撞在一起，
//     一个错都不报——标准 YCSB 的 key（"user" + 64 位哈希，最长 24 字符）正是这种。
//
// 把宽度从 10 调到 24 只是把悬崖往后推，没有取消它。真正的修法是把编码交回调用方：
// 想让范围查询有**数值**语义，就由调用方把整数编码成定长字节串（见 internal/client 的
// KeyPadWidth）。在"定长十进制"这个定义域里补零是单射的、可逆的；在任意字符串上不是。
//
// 记录格式本来就支持变长 key——20 字节头里带着 keySize，recover.go 就是按它步进的，
// 所以这个改动不需要动盘上格式。

// ReservedKeyPrefix 是元数据占用的首字节。恢复用的 applied index 与用户数据同库，
// 靠首字节 0x00 区分（见 IsMetaKey），所以用户 key 不能以它开头。
const ReservedKeyPrefix = byte(0)

// ErrReservedKey 表示 key 落在存储层保留的命名空间里。
var ErrReservedKey = errors.New("key must not start with a NUL byte: that prefix is reserved for store metadata")

// ValidateKey 检查一个用户 key 能否安全存储。
//
// 只有一条限制，而且是显式报错而不是静默改写——静默改写正是此前那个 bug 的形态。
func ValidateKey(key string) error {
	if len(key) > 0 && key[0] == ReservedKeyPrefix {
		return ErrReservedKey
	}
	return nil
}

var ErrKeyNotFound = errors.New("key not found")

type Persister struct {
	db   *grocksdb.DB
	ro   *grocksdb.ReadOptions
	wo   *grocksdb.WriteOptions
	muRO sync.Mutex
	muWO sync.Mutex
}

// disableWAL 关掉存储引擎自己的预写日志，供 PASV 使用。
//
// 设成包级而非 Init 的参数，是因为 GC 期间还会新建存储引擎实例——那些实例必须
// 继承同样的设置，否则一轮 GC 之后 PASV 会悄悄变回 Original。
var disableWAL bool

// SetDisableWAL 必须在任何 Init 之前调用。
func SetDisableWAL(v bool) { disableWAL = v }

// blockCacheBytes 是 RocksDB 块缓存的大小，0 表示不开。
//
// **为什么要做成包级变量而不是 Init 的参数**：GC 每一轮都新建 persister
// （gc_first.go、gc_merge.go），装快照也新建一个（snapshot.go）。参数形式要求
// 每一处都记得传同一个值，漏一处的后果是**一轮 GC 之后配置悄悄变回去**，
// 而且没有任何迹象——disableWAL 上面那行注释记的就是这个坑。
// 包级变量让"设一次、对所有 persister 生效"成为默认。
var blockCacheBytes int

// SetBlockCacheMB 必须在任何 Init 之前调用。0 表示不开块缓存（历史默认）。
//
// **这不是一个无关紧要的调参。** 六个生产调用点一直传 disableCache=true，于是
// 走的是 SetNoBlockCache(true) + CacheIndexAndFilterBlocks(false)：四个系统的
// 每一次点读都要从文件里读索引块、过滤块、数据块，一个都不缓存。
// 那些读落在操作系统页缓存里，所以 10GB 规模下量不出代价——而它同时把
// **KV 分离的结构优势一起抹掉了**：Nezha 的 LSM 只存 key→9 字节偏移
// （10GB / 1KB 的数据约 300MB），Original 的要装完整 10GB。
// 有块缓存时前者整个进得去、后者进不去，这正是"两跳打赢一跳"的机制；
// 关掉之后两边都走文件读，Nezha 白多一跳。
func SetBlockCacheMB(mb int) {
	if mb < 0 {
		mb = 0
	}
	blockCacheBytes = mb << 20
}

func (p *Persister) Init(path string, disableCache bool) (*Persister, error) {
	var err error
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	opts := grocksdb.NewDefaultOptions()

	// SetBlockCacheMB 给了大小就以它为准，压过调用方传的 disableCache——
	// 六个调用点全都写死了 true，要开缓存只能从外面推翻它。
	if blockCacheBytes > 0 {
		bbto.SetBlockCache(grocksdb.NewLRUCache(uint64(blockCacheBytes)))
		bbto.SetCacheIndexAndFilterBlocks(true)
	} else if disableCache {
		// 完全禁用所有缓存
		bbto.SetNoBlockCache(true)               // 禁用块缓存
		bbto.SetCacheIndexAndFilterBlocks(false) // 禁用索引和过滤器块的缓存
		// bbto.SetFilterPolicy(nil)                // 禁用 Bloom Filter
		opts.SetAllowMmapReads(false) // 关闭预读/内存映射读取
	} else {
		// 启用缓存
		bbto.SetBlockCache(grocksdb.NewLRUCache(3 << 30))
		bbto.SetCacheIndexAndFilterBlocks(true)
	}

	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	p.db, err = grocksdb.OpenDb(opts, path)
	if err != nil {
		return nil, fmt.Errorf("open db failed: %w", err)
	}

	p.wo = grocksdb.NewDefaultWriteOptions()
	// PASV 的做法：去掉存储引擎自己的 WAL，消除"Raft 日志 + 存储引擎 WAL"这层
	// 双重日志。Raft 日志与 SSTable 的冗余仍在，所以它相对 Original 只有有限的
	// 改善（论文实测 +26.5%）。
	if disableWAL {
		p.wo.DisableWAL(true)
	}
	p.ro = grocksdb.NewDefaultReadOptions()

	if disableCache {
		p.ro.SetFillCache(false) // 防止读取操作填充缓存
	}

	p.muRO = sync.Mutex{}
	p.muWO = sync.Mutex{}

	return p, nil
}

func (p *Persister) Close() {
	p.muRO.Lock()
	defer p.muRO.Unlock()
	if p.ro != nil {
		p.ro.Destroy()
		p.ro = nil
	}
	p.muWO.Lock()
	defer p.muWO.Unlock()
	if p.wo != nil {
		p.wo.Destroy()
		p.wo = nil
	}
	if p.db != nil {
		p.db.Close()
		p.db = nil
	}
}

// RocksDB 里存的东西现在有两种，靠首字节区分。
//
// 原先靠长度判断（Get_opt 检查 len != 8 即报错），那在只存偏移时够用，
// 一旦开始内联 value 就会撞车：一个 7 字节的 value 加上标记正好也是 8 字节。
// 标记字节让两者无论长度如何都能分开。
const (
	TagOffset = byte(0x00) // 其后 8 字节为 valuelog 偏移（KV 分离）
	TagInline = byte(0x01) // 其后即 value 本身（小值内联）
)

// DecodeOffsetRecord 从存储引擎的一条记录里取出 valuelog 偏移。
//
// 记录格式是 [Tag, offset8] 共 9 字节，偏移在 [1:]。这件事此前散落在各处各写
// 一遍，于是同一个错误犯了两次：SCAN 的 ReadValueFromNewFile 和 GC 的主循环
// 都按 [0:8] 解析，把标记字节当成了偏移的最低位——算出来的是"真实偏移左移
// 8 位再截断"，看着像个合法偏移，seek 过去却落在文件的任意位置。
//
// GC 因此读到 EOF 而失败，SCAN 因此返回空。所有需要偏移的地方都应当走这里，
// 不要再各自解析。
func DecodeOffsetRecord(raw []byte) (int64, error) {
	if len(raw) == 0 {
		return 0, errors.New("empty offset record")
	}
	switch raw[0] {
	case TagOffset:
		if len(raw) != 9 {
			return 0, fmt.Errorf("invalid offset record size: %d", len(raw))
		}
		return int64(binary.LittleEndian.Uint64(raw[1:])), nil
	case TagInline:
		return 0, errors.New(ErrInlineValue)
	}
	return 0, fmt.Errorf("unknown record tag: 0x%02x", raw[0])
}

func (p *Persister) Put_opt(key string, value int64) {
	// 不要创建新的 wo，使用对象中已经配置好的
	// wo := grocksdb.NewDefaultWriteOptions()
	// defer wo.Destroy()

	valueBytes := make([]byte, 9)
	valueBytes[0] = TagOffset
	binary.LittleEndian.PutUint64(valueBytes[1:], uint64(value))

	p.muWO.Lock()
	defer p.muWO.Unlock()
	err := p.db.Put(p.wo, []byte(key), valueBytes)
	if err != nil {
		util.EPrintf("Put key %v value ** failed, err: %v", key, err)
	}
}

func (p *Persister) Put(key string, value string) {
	// 不要创建新的 wo，使用对象中已经配置好的
	// wo := grocksdb.NewDefaultWriteOptions()
	// defer wo.Destroy()
	p.muWO.Lock()
	defer p.muWO.Unlock()
	err := p.db.Put(p.wo, []byte(key), []byte(value))
	if err != nil {
		util.EPrintf("Put key %v value ** failed, err: %v", key, err)
	}
}

// PutInline 把小 value 直接存进存储引擎，不经 valuelog。
// 读取时一次点查即可拿到 value，省去"查偏移再读日志文件"的第二次 I/O；
// 且它随存储引擎持久化，重启后依然有效，不像内存缓存要等 GC 重建。
func (p *Persister) PutInline(key string, value string) {
	buf := make([]byte, 1+len(value))
	buf[0] = TagInline
	copy(buf[1:], value)

	p.muWO.Lock()
	defer p.muWO.Unlock()
	if err := p.db.Put(p.wo, []byte(key), buf); err != nil {
		util.EPrintf("PutInline key %v failed, err: %v", key, err)
	}
}

// RecordKind 是一条记录的形态。
type RecordKind int

const (
	RecordMissing RecordKind = iota // 这个 key 不在这个存储引擎里
	RecordInline                    // value 就内联在记录里
	RecordOffset                    // 记录里只有 valuelog 偏移
)

// GetRecord 取一条记录，**一次 db.Get 同时回答"是内联还是偏移"和"内容是什么"**。
//
// 这个方法存在的理由是一个实测到的缺陷：原先点读路径先调 GetInline 问一次
// "是不是内联"，不是就回落到 Get_opt 再查一次拿偏移——而两者都是完整的
// db.Get。于是**开了 -inlinePlacement 而 value 大于内联阈值时，每一次点读都要
// 查两遍 RocksDB**，第一遍注定落空。
// 2026-09-19 的 10GB 实测：1KB / 4KB 两档的点读吞吐因此比不开这个开关低
// 9.0% / 8.5%，而 16KB / 256KB 归零——正是"每次读的固定开销被大 value 摊薄"
// 的形状。
//
// 分流所需的信息本来就在同一条记录里，问两遍是把一件事拆成了两次 I/O。
func (p *Persister) GetRecord(key string) (RecordKind, string, int64, error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	slice, err := p.db.Get(ro, []byte(key))
	if err != nil {
		util.EPrintf("Get key %s failed, err: %s", key, err)
		return RecordMissing, "", 0, err
	}
	defer slice.Free()
	// 判"有没有这个 key"用 Exists()，理由同 Get：长度为 0 既可能是缺键，
	// 也可能是一个内容为空串的 value。
	if !slice.Exists() {
		return RecordMissing, "", 0, nil
	}
	b := slice.Data()
	if len(b) == 0 {
		return RecordMissing, "", 0, errors.New("empty record")
	}
	switch b[0] {
	case TagInline:
		// slice 在本函数返回时就被 Free，所以必须拷一份出去，不能只借它的底层数组。
		return RecordInline, string(b[1:]), 0, nil
	case TagOffset:
		off, err := DecodeOffsetRecord(b)
		if err != nil {
			return RecordMissing, "", 0, err
		}
		return RecordOffset, "", off, nil
	}
	return RecordMissing, "", 0, fmt.Errorf("unknown record tag: 0x%02x", b[0])
}

// GetInline 取内联 value。第二个返回值为 false 表示这个 key 不是内联存储的
// （或不存在），调用方应回落到偏移查找路径。
//
// **点读路径不要再用它**：它只回答"是不是内联"，不是的话调用方还得再查一遍，
// 那正是 GetRecord 那段注释里说的双查找。保留它是因为它的语义对只关心内联的
// 调用方（单测、诊断）更直白。
func (p *Persister) GetInline(key string) (string, bool) {
	kind, v, _, err := p.GetRecord(key)
	if err != nil || kind != RecordInline {
		return "", false
	}
	return v, true
}

func (p *Persister) Get_opt(key string) (int64, error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	// p.muRO.Lock()
	// defer p.muRO.Unlock()
	slice, err := p.db.Get(ro, []byte(key))
	if err != nil {
		util.EPrintf("Get key %s failed, err: %s", key, err)
		return 0, err
	}
	defer slice.Free()
	valueBytes := slice.Data()
	// if slice.Size() == 0 {
	// 	return -1, nil
	// }
	if !slice.Exists() {
		// return -1, ErrKeyNotFound
		return -1, nil
	}
	// 只认标记，不做长度兼容。曾经写过一个"len==8 视为旧格式偏移"的分支，
	// 它恰好把要防的碰撞又放了回来：7 字节的内联 value 加上标记正好 8 字节，
	// 于是被当成偏移解析。而且旧格式本身就有歧义——偏移量为 1 时，
	// 小端编码的首字节就是 0x01，与内联标记无法区分。
	// TagInline 表示这个 key 的 value 内联在存储引擎里，没有偏移可言；
	// 调用方应改走 GetInline，拿到该错误说明分流逻辑漏了一处。
	return DecodeOffsetRecord(valueBytes)
}

func (p *Persister) Get(key string) (string, error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	// p.muRO.Lock()
	// defer p.muRO.Unlock()
	slice, err := p.db.Get(ro, []byte(key))
	if err != nil {
		util.EPrintf("Get key %s failed, err: %s", key, err)
		return "", err
	}
	defer slice.Free()
	// 判"有没有这个 key"必须用 Exists()，不能用 Size() == 0。
	//
	// 长度为 0 有**两种**情形：key 不存在，以及 key 存在而 value 是空串。按长度判会把
	// 后者当成前者，于是基线路径（-kvSeparation=false，value 直接存在存储引擎里）下
	// 一条已提交的空 value 读回来是 ErrNoKey——写进去了、提交了，却报"没有这个 key"。
	// 空 value 没有任何地方拒绝：PutInRaft 只校验 key（raft.ValidateKey）。
	// 同一个文件里的 Get_opt 用的就是 Exists()，两个姊妹函数对"存在"的判据本来不一致。
	if !slice.Exists() {
		return "", ErrKeyNotFound
	}
	return string(slice.Data()), nil
}

// ScanRange 执行范围查询，使用固定长度的string类型键
func (p *Persister) ScanRange_opt(startKey, endKey string) (map[string]int64, error) {
	// p.muRO.Lock()
	// defer p.muRO.Unlock()
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	result := make(map[string]int64)

	it := p.db.NewIterator(ro)
	defer it.Close()

	for it.Seek([]byte(startKey)); it.Valid(); it.Next() { // Valid判断键是否存在，不存在就直接下一个
		key := it.Key()
		value := it.Value()
		// Free 立即调用，不能 defer：Go 对循环内的 defer 不做开放编码优化，
		// 每个都是堆上分配的 _defer 记录、压到**函数返回**才释放，于是数量是
		// O(扫描范围)。一次 gapkey=1000 的 SCAN 就是 2000 条记录，而 benchmark
		// 要做二十万次这样的调用。gc_absorb.go 用的就是立即 Free。
		// 检查是否超出范围
		if string(key.Data()) > endKey {
			key.Free()
			value.Free()
			break
		}
		if IsMetaKey(key.Data()) { // 恢复用的 applied index，不是用户数据
			key.Free()
			value.Free()
			continue
		}

		// 解析值
		k := string(key.Data())
		valueInt64, perr := parseValueInt64(value.Data())
		key.Free()
		value.Free()
		if perr != nil {
			return nil, fmt.Errorf("error parsing value: %v", perr)
		}

		result[k] = valueInt64
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %v", err)
	}

	return result, nil
}

// parseValueInt64 解析值为 int64。
// 原先要求恰好 8 字节，加上标记字节后记录变成 9 字节，于是这个函数必然失败。
func parseValueInt64(value []byte) (int64, error) {
	return DecodeOffsetRecord(value)
}

func (p *Persister) GetDb() (db *grocksdb.DB) {
	return p.db
}

func (p *Persister) ScanRange(startKey, endKey string) (map[string]string, error) {
	// p.muRO.Lock()
	// defer p.muRO.Unlock()
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	result := make(map[string]string)

	// fmt.Printf("startkey:%v,endkey:%v\n", startKey, endKey)

	it := p.db.NewIterator(ro)
	defer it.Close()

	for it.Seek([]byte(startKey)); it.Valid(); it.Next() {
		key := it.Key()
		value := it.Value()
		// Free 立即调用，不能 defer：Go 对循环内的 defer 不做开放编码优化，
		// 每个都是堆上分配的 _defer 记录、压到**函数返回**才释放，于是数量是
		// O(扫描范围)。一次 gapkey=1000 的 SCAN 就是 2000 条记录，而 benchmark
		// 要做二十万次这样的调用。gc_absorb.go 用的就是立即 Free。
		// 检查是否超出范围
		if string(key.Data()) > endKey {
			key.Free()
			value.Free()
			break
		}
		if IsMetaKey(key.Data()) { // 恢复用的 applied index，不是用户数据
			key.Free()
			value.Free()
			continue
		}

		// 直接使用字符串值
		k := string(key.Data())
		valueString := string(value.Data())
		key.Free()
		value.Free()

		result[k] = valueString
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %v", err)
	}

	return result, nil
}

// ---- crash recovery: the applied log index travels with the data ----
//
// The index is kept inside the store rather than in a state file because it must agree
// exactly with the rows that made it in: a crash between two files would replay or skip an
// entry. One WriteBatch gives RocksDB atomicity, and under -syncWAL the marker has the same
// durability as the data at no extra fsync.
//
// The key starts with 0x00, which ValidateKey refuses for user keys, so there is no
// collision; GC's full-store iteration and range scans skip it (see IsMetaKey).
const appliedIndexKey = "\x00applied_index"

// IsMetaKey reports whether a store key is recovery metadata rather than user data.
func IsMetaKey(k []byte) bool {
	return len(k) > 0 && k[0] == 0
}

func encodeApplied(applied int) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(applied))
	return b
}

// StoreRow 是一条待写入存储引擎的行。Value 里**已经带好标记字节**，
// 由下面三个 EncodeXxxRow 生成——标记的编码只在那三处，不要在调用方重写一遍
// （散落各处各写一遍这件事已经犯过：见 DecodeOffsetRecord 的注释）。
type StoreRow struct {
	Key   []byte
	Value []byte
}

// EncodeOffsetRow 生成 [TagOffset, offset8] 的行（KV 分离：库里只存偏移）。
func EncodeOffsetRow(key string, offset int64) StoreRow {
	v := make([]byte, 9)
	v[0] = TagOffset
	binary.LittleEndian.PutUint64(v[1:], uint64(offset))
	return StoreRow{Key: []byte(key), Value: v}
}

// EncodeInlineRow 生成 [TagInline, value] 的行（小值内联）。
func EncodeInlineRow(key, value string) StoreRow {
	v := make([]byte, 1+len(value))
	v[0] = TagInline
	copy(v[1:], value)
	return StoreRow{Key: []byte(key), Value: v}
}

// EncodeValueRow 生成裸 value 的行（基线：不做 KV 分离，value 直接进库，无标记）。
func EncodeValueRow(key, value string) StoreRow {
	return StoreRow{Key: []byte(key), Value: []byte(value)}
}

// WriteRowsApplied 把 N 行数据与**一个** applied 标记写成一个 WriteBatch。
//
// 为什么要有批量版：每条 apply 各写一次 db.Write，实测 9.1µs，是单 goroutine 的
// apply 循环每条 17.1µs 里最大的一块；而每次 Write 还要**额外重写一遍 applied
// 标记**，那个标记只有一批里的最后一条有意义。2026-09-20 的并发扫描实测，
// 写吞吐在并发 100→400 之间完全压平（55,672 / 55,488 / 55,437 ops/s），
// 而 1/avg_busy = 57~58K 正是那条天花板。
//
// **批内顺序等同于逐条应用**：WriteBatch 按插入顺序生效，同一个 key 在一批里
// 被写两次时后者胜——与顺序应用同一结果。applied 标记放在最后一个 Put，
// 所以它也不会被同批里更早的行盖掉。
//
// 崩溃语义比逐条更粗但仍然正确：一批要么全进要么全不进，而重启会从库里的
// applied 标记之后重放 Raft 日志，重放是幂等的（同 key 同偏移）。
func (p *Persister) WriteRowsApplied(rows []StoreRow, applied int) error {
	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()
	for i := range rows {
		wb.Put(rows[i].Key, rows[i].Value)
	}
	wb.Put([]byte(appliedIndexKey), encodeApplied(applied))
	p.muWO.Lock()
	defer p.muWO.Unlock()
	return p.db.Write(p.wo, wb)
}

// writeWithApplied writes one data row and the applied index in a single WriteBatch.
// 它现在是 WriteRowsApplied 的单行特例，两者共用一份实现。
func (p *Persister) writeWithApplied(key []byte, value []byte, applied int) error {
	if key == nil {
		return p.WriteRowsApplied(nil, applied)
	}
	return p.WriteRowsApplied([]StoreRow{{Key: key, Value: value}}, applied)
}

// PutOffsetApplied is Put_opt plus the applied index, as one atomic batch.
func (p *Persister) PutOffsetApplied(key string, offset int64, applied int) {
	valueBytes := make([]byte, 9)
	valueBytes[0] = TagOffset
	binary.LittleEndian.PutUint64(valueBytes[1:], uint64(offset))
	if err := p.writeWithApplied([]byte(key), valueBytes, applied); err != nil {
		util.EPrintf("PutOffsetApplied key %v failed, err: %v", key, err)
	}
}

// PutInlineApplied is PutInline plus the applied index.
func (p *Persister) PutInlineApplied(key string, value string, applied int) {
	buf := make([]byte, 1+len(value))
	buf[0] = TagInline
	copy(buf[1:], value)
	if err := p.writeWithApplied([]byte(key), buf, applied); err != nil {
		util.EPrintf("PutInlineApplied key %v failed, err: %v", key, err)
	}
}

// PutValueApplied is Put (baseline: the value itself goes into the store) plus the applied index.
func (p *Persister) PutValueApplied(key string, value string, applied int) {
	if err := p.writeWithApplied([]byte(key), []byte(value), applied); err != nil {
		util.EPrintf("PutValueApplied key %v failed, err: %v", key, err)
	}
}

// SetApplied advances only the applied index (no-op entries, or rows written to another store).
func (p *Persister) SetApplied(applied int) {
	if err := p.writeWithApplied(nil, nil, applied); err != nil {
		util.EPrintf("SetApplied %d failed, err: %v", applied, err)
	}
}

// GetApplied reads the applied index back; (0, false) when the store has none.
func (p *Persister) GetApplied() (int, bool, error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	v, err := p.db.Get(ro, []byte(appliedIndexKey))
	if err != nil {
		return 0, false, err
	}
	defer v.Free()
	if !v.Exists() || v.Size() != 8 {
		return 0, false, nil
	}
	return int(binary.LittleEndian.Uint64(v.Data())), true, nil
}
