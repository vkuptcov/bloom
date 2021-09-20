package bloom

import (
	"io"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/pkg/errors"
)

type InMemoryBlooms struct {
	filterParams FilterParams

	filters []*bloom.BloomFilter
	mutex   *sync.Mutex
}

func NewInMemory(filterParams FilterParams) *InMemoryBlooms {
	filters := make([]*bloom.BloomFilter, filterParams.BucketsCount)
	bitsCount, hashFunctionsNumber := filterParams.EstimatedBucketParameters()
	for i := 0; i < filterParams.BucketsCount; i++ {
		filters[i] = bloom.New(bitsCount, hashFunctionsNumber)
	}

	return &InMemoryBlooms{
		filterParams: filterParams,
		filters:      filters,
		mutex:        &sync.Mutex{},
	}
}

func (b *InMemoryBlooms) Add(data []byte) *InMemoryBlooms {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.underlyingFilter(data).Add(data)
	return b
}

func (b *InMemoryBlooms) AddString(data string) *InMemoryBlooms {
	return b.Add([]byte(data))
}

func (b *InMemoryBlooms) AddUint16(i uint16) *InMemoryBlooms {
	return b.Add(uint16ToByte(i))
}

func (b *InMemoryBlooms) AddUint32(i uint32) *InMemoryBlooms {
	return b.Add(uint32ToByte(i))
}

func (b *InMemoryBlooms) AddUint64(i uint64) *InMemoryBlooms {
	return b.Add(uint64ToByte(i))
}

func (b *InMemoryBlooms) Test(data []byte) bool {
	return b.underlyingFilter(data).Test(data)
}

func (b *InMemoryBlooms) TestString(data string) bool {
	return b.Test([]byte(data))
}

func (b *InMemoryBlooms) TestUint16(i uint16) bool {
	return b.Test(uint16ToByte(i))
}

func (b *InMemoryBlooms) TestUint32(i uint32) bool {
	return b.Test(uint32ToByte(i))
}

func (b *InMemoryBlooms) TestUint64(i uint64) bool {
	return b.Test(uint64ToByte(i))
}

func (b *InMemoryBlooms) Restore(bucketID int, stream io.Reader) error {
	_, err := b.filters[bucketID].ReadFrom(stream)
	return err
}

func (b *InMemoryBlooms) WriteTo(bucketID int, stream io.Writer) (int64, error) {
	return b.filters[bucketID].WriteTo(stream)
}

func (b *InMemoryBlooms) AddFrom(bucketID int, stream io.Reader) error {
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

func (b *InMemoryBlooms) BucketSize() uint32 {
	return uint32(b.filters[0].Cap())
}

func (b *InMemoryBlooms) underlyingFilter(data []byte) *bloom.BloomFilter {
	return b.filters[b.filterParams.BucketID(data)]
}
