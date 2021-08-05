package bloom

import (
	"io"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/pkg/errors"
)

type inMemoryBlooms struct {
	filterParams FilterParams

	filters []*bloom.BloomFilter
	mutex   *sync.Mutex
}

func NewInMemory(filterParams FilterParams) *inMemoryBlooms {
	filters := make([]*bloom.BloomFilter, filterParams.BucketsCount)
	bitsCount, hashFunctionsNumber := filterParams.EstimatedBucketParameters()
	for i := uint32(0); i < filterParams.BucketsCount; i++ {
		filters[i] = bloom.New(bitsCount, hashFunctionsNumber)
	}

	return &inMemoryBlooms{
		filterParams: filterParams,
		filters:      filters,
		mutex:        &sync.Mutex{},
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

func (b *inMemoryBlooms) AddUint16(i uint16) *inMemoryBlooms {
	return b.Add(uint16ToByte(i))
}

func (b *inMemoryBlooms) AddUint32(i uint32) *inMemoryBlooms {
	return b.Add(uint32ToByte(i))
}

func (b *inMemoryBlooms) AddUint64(i uint64) *inMemoryBlooms {
	return b.Add(uint64ToByte(i))
}

func (b *inMemoryBlooms) Test(data []byte) bool {
	return b.underlyingFilter(data).Test(data)
}

func (b *inMemoryBlooms) TestString(data string) bool {
	return b.Test([]byte(data))
}

func (b *inMemoryBlooms) TestUint16(i uint16) bool {
	return b.Test(uint16ToByte(i))
}

func (b *inMemoryBlooms) TestUint32(i uint32) bool {
	return b.Test(uint32ToByte(i))
}

func (b *inMemoryBlooms) TestUint64(i uint64) bool {
	return b.Test(uint64ToByte(i))
}

func (b *inMemoryBlooms) Restore(bucketID uint64, stream io.Reader) error {
	_, err := b.filters[bucketID].ReadFrom(stream)
	return err
}

func (b *inMemoryBlooms) AddFrom(bucketID uint64, stream io.Reader) error {
	var restored bloom.BloomFilter
	if _, restoreErr := restored.ReadFrom(stream); restoreErr != nil {
		return errors.Wrap(restoreErr, "filter restore error")
	}
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if mergeErr := b.filters[bucketID].Merge(&restored); mergeErr != nil {
		return errors.Wrap(mergeErr, "filter merge error")
	}
	return nil
}

func (b *inMemoryBlooms) BucketSize() uint32 {
	return uint32(b.filters[0].Cap())
}

func (b *inMemoryBlooms) underlyingFilter(data []byte) *bloom.BloomFilter {
	return b.filters[b.filterParams.BucketID(data)]
}
