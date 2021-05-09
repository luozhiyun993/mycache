package mycache

import (
	"fmt"
	"sync"

	xxhash "github.com/cespare/xxhash/v2"
)

const bucketsCount = 512

const chunkSize = 64 * 1024

const bucketSizeBits = 40

const genSizeBits = 64 - bucketSizeBits

const maxGen = 1<<genSizeBits - 1

const maxBucketSize uint64 = 1 << bucketSizeBits

type Cache struct {
	buckets [bucketsCount]bucket
}

type bucket struct {
	// 读写锁
	mu sync.RWMutex

	// 二维数组，存放数据的地方，是一个环形链表
	chunks [][]byte

	// 索引字典
	m map[uint64]uint64

	// 索引值
	idx uint64

	// chunks 被重写的次数
	gen uint64
}

func (b *bucket) Init(maxBytes uint64) {
	if maxBytes == 0 {
		panic(fmt.Errorf("maxBytes cannot be zero"))
	}
	if maxBytes >= maxBucketSize {
		panic(fmt.Errorf("too big maxBytes=%d; should be smaller than %d", maxBytes, maxBucketSize))
	}
	// 初始化 Chunks 大小
	//maxChunks := (maxBytes + chunkSize - 1) / chunkSize
	maxChunks := 1
	b.chunks = make([][]byte, maxChunks)
	b.m = make(map[uint64]uint64)
	// 初始化 chunk
	b.Reset()
}

func New(maxBytes int) *Cache {
	if maxBytes <= 0 {
		panic(fmt.Errorf("maxBytes must be greater than 0; got %d", maxBytes))
	}
	var c Cache
	// 算出每个桶的大小，加上一个 bucketsCount 相当于有
	maxBucketBytes := uint64((maxBytes + bucketsCount - 1) / bucketsCount)
	for i := range c.buckets[:] {
		c.buckets[i].Init(maxBucketBytes)
	}
	return &c
}

func (c *Cache) Set(k, v []byte) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	c.buckets[idx].Set(k, v, h)
}

func (c *Cache) Get(k []byte) []byte {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	dst, _ := c.buckets[idx].Get(k, h)
	return dst
}

func (c *Cache) Reset() {
	for i := range c.buckets[:] {
		c.buckets[i].Reset()
	}
}

func (b *bucket) Set(k, v []byte, h uint64) {
	// 限定 k v 大小不能超过 2bytes
	if len(k) >= (1<<16) || len(v) >= (1<<16) {
		return
	}
	// 4个byte 设置每条数据的数据头
	var kvLenBuf [4]byte
	kvLenBuf[0] = byte(uint16(len(k)) >> 8)
	kvLenBuf[1] = byte(len(k))
	kvLenBuf[2] = byte(uint16(len(v)) >> 8)
	kvLenBuf[3] = byte(len(v))
	kvLen := uint64(len(kvLenBuf) + len(k) + len(v))
	// 校验一下大小
	if kvLen >= chunkSize {
		return
	}

	b.mu.Lock()
	// 当前索引位置
	idx := b.idx
	// 存放完数据后索引的位置
	idxNew := idx + kvLen
	// 根据索引找到在 chunks 的位置
	chunkIdx := idx / chunkSize
	chunkIdxNew := idxNew / chunkSize
	// 新的索引是否超过当前索引
	// 因为还有chunkIdx等于chunkIdxNew情况，所以需要先判断一下
	if chunkIdxNew > chunkIdx {
		// 校验是否新索引已到chunks数组的边界
		// 已到边界，那么循环链表从头开始
		if chunkIdxNew >= uint64(len(b.chunks)) {
			idx = 0
			idxNew = kvLen
			chunkIdx = 0
			b.gen++
			// 当 gen 等于 1<<genSizeBits时，才会等于0
			// 也就是用来限定 gen 的边界为1<<genSizeBits
			if b.gen&((1<<genSizeBits)-1) == 0 {
				b.gen++
			}
		} else {
			// 未到 chunks数组的边界,从下一个chunk开始
			idx = chunkIdxNew * chunkSize
			idxNew = idx + kvLen
			chunkIdx = chunkIdxNew
		}
		// 重置 chunks[chunkIdx]
		b.chunks[chunkIdx] = b.chunks[chunkIdx][:0]
	}
	chunk := b.chunks[chunkIdx]
	if chunk == nil {
		chunk = getChunk()
		// 清空切片
		chunk = chunk[:0]
	}
	// 将数据 append 到 chunk 中
	chunk = append(chunk, kvLenBuf[:]...)
	chunk = append(chunk, k...)
	chunk = append(chunk, v...)
	b.chunks[chunkIdx] = chunk
	// 因为 idx 不能超过bucketSizeBits，所以用一个 uint64 同时表示gen和idx
	// 所以高于bucketSizeBits位置表示gen
	// 低于bucketSizeBits位置表示idx
	b.m[h] = idx | (b.gen << bucketSizeBits)
	b.idx = idxNew
	b.mu.Unlock()
}

func (b *bucket) Get(k []byte, h uint64) ([]byte, bool) {
	var dst []byte
	found := false
	b.mu.RLock()
	v := b.m[h]
	bGen := b.gen & ((1 << genSizeBits) - 1)
	if v > 0 {
		// 高于bucketSizeBits位置表示gen
		gen := v >> bucketSizeBits
		// 低于bucketSizeBits位置表示idx
		idx := v & ((1 << bucketSizeBits) - 1)
		// 这里说明chunks还没被写满
		if gen == bGen && idx < b.idx ||
			// 这里说明chunks已被写满，并且当前数据没有被覆盖
			gen+1 == bGen && idx >= b.idx ||
			// 这里是边界条件gen已是最大，并且chunks已被写满bGen从1开始，，并且当前数据没有被覆盖
			gen == maxGen && bGen == 1 && idx >= b.idx {
			chunkIdx := idx / chunkSize
			// chunk 索引位置不能超过 chunks 数组长度
			if chunkIdx >= uint64(len(b.chunks)) {
				goto end
			}
			// 找到数据所在的 chunk
			chunk := b.chunks[chunkIdx]
			// 通过取模找到该key 对应的数据在 chunk 中的位置
			idx %= chunkSize
			if idx+4 >= chunkSize {
				goto end
			}
			// 前 4bytes 是数据头
			kvLenBuf := chunk[idx : idx+4]
			// 通过数据头算出键值的长度
			keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
			valLen := (uint64(kvLenBuf[2]) << 8) | uint64(kvLenBuf[3])
			idx += 4
			if idx+keyLen+valLen >= chunkSize {
				goto end
			}
			// 如果键值是一致的，表示找到该数据
			if string(k) == string(chunk[idx:idx+keyLen]) {
				idx += keyLen
				// 返回该键对应的值
				dst = append(dst, chunk[idx:idx+valLen]...)
				found = true
			}
		}
	}
end:
	b.mu.RUnlock()
	return dst, found
}

func (b *bucket) Reset() {
	b.mu.Lock()
	chunks := b.chunks
	// 遍历 chunks
	for i := range chunks {
		// 将 chunk 中的内存归还到缓存中
		putChunk(chunks[i])
		chunks[i] = nil
	}
	// 删除索引字典中所有的数据
	bm := b.m
	for k := range bm {
		delete(bm, k)
	}
	b.idx = 0
	b.gen = 1
	b.mu.Unlock()
}
