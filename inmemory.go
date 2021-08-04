package bloom

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
)

type inMemoryBlooms struct {
	bucketsCount   uint32
	totalElements  uint64
	falsePositives float64

	filters []*bloom.BloomFilter
	mutex   *sync.Mutex
}

func NewInMemory(bucketsCount uint32, totalElements uint64, falsePositives float64) *inMemoryBlooms {
	bucketSize := totalElements / uint64(bucketsCount)
	filters := make([]*bloom.BloomFilter, bucketSize)
	for i := uint64(0); i < bucketSize; i++ {
		filters[i] = bloom.NewWithEstimates(uint(bucketSize), falsePositives)
	}

	return &inMemoryBlooms{
		bucketsCount:   bucketsCount,
		totalElements:  totalElements,
		falsePositives: falsePositives,
		filters:        filters,
		mutex:          &sync.Mutex{},
	}
}

func (b *inMemoryBlooms) Add(data []byte) *inMemoryBlooms {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.underlyingFilter(data).Add(data)
	return b
}

func (b *inMemoryBlooms) AddString(data string) *inMemoryBlooms {
	return b.Add([]byte(data))
}

func (b *inMemoryBlooms) AddUint32(i uint32) *inMemoryBlooms {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, i)
	return b.Add(data)
}

func (b *inMemoryBlooms) AddUint64(i uint64) *inMemoryBlooms {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, i)
	return b.Add(data)
}

func (b *inMemoryBlooms) Test(data []byte) bool {
	return b.underlyingFilter(data).Test(data)
}

func (b *inMemoryBlooms) Restore(bucketID uint64, stream io.Reader) error {
	_, err := b.filters[bucketID].ReadFrom(stream)
	return err
}

func (b *inMemoryBlooms) Write(bucketID uint64) error {
	buf := &bytes.Buffer{}
	_, err := b.filters[bucketID].WriteTo(buf)
	str := buf.String()
	println(str)
	return err
}

func (b *inMemoryBlooms) BucketSize() uint32 {
	return uint32(b.filters[0].Cap())
}

func (b *inMemoryBlooms) underlyingFilter(data []byte) *bloom.BloomFilter {
	return b.filters[b.bucketID(data)]
}

func (b *inMemoryBlooms) bucketID(data []byte) uint64 {
	return bucketID(data, uint64(b.bucketsCount))
}

func bucketID(data []byte, bucketsCount uint64) uint64 {
	return bloom.Locations(data, 1)[0] % bucketsCount
}
