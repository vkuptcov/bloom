package bloom

import (
	"encoding/binary"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
)

type DBloom struct {
	bucketsCount   uint32
	totalElements  uint64
	falsePositives float64

	filters []*bloom.BloomFilter
	mutex   *sync.Mutex
}

func NewDBloom(bucketsCount uint32, totalElements uint64, falsePositives float64) *DBloom {
	bucketSize := totalElements / uint64(bucketsCount)
	filters := make([]*bloom.BloomFilter, bucketSize)
	for i := uint64(0); i < bucketSize; i++ {
		filters[i] = bloom.NewWithEstimates(uint(bucketSize), falsePositives)
	}

	return &DBloom{
		bucketsCount:   bucketsCount,
		totalElements:  totalElements,
		falsePositives: falsePositives,
		filters:        filters,
		mutex:          &sync.Mutex{},
	}
}

func (b *DBloom) Add(data []byte) *DBloom {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.underlyingFilter(data).Add(data)
	return b
}

func (b *DBloom) AddString(data string) *DBloom {
	return b.Add([]byte(data))
}

func (b *DBloom) AddUint32(i uint32) *DBloom {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, i)
	return b.Add(data)
}

func (b *DBloom) AddUint64(i uint64) *DBloom {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, i)
	return b.Add(data)
}

func (b *DBloom) Test(data []byte) bool {
	return b.underlyingFilter(data).Test(data)
}

func (b *DBloom) BucketSize() uint32 {
	return uint32(b.filters[0].Cap())
}

func (b *DBloom) underlyingFilter(data []byte) *bloom.BloomFilter {
	return b.filters[b.bucketID(data)]
}

func (b *DBloom) bucketID(data []byte) uint64 {
	return bucketID(data, uint64(b.bucketsCount))
}

func bucketID(data []byte, bucketsCount uint64) uint64 {
	return bloom.Locations(data, 1)[0] % bucketsCount
}
