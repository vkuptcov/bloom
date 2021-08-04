package bloom

import (
	"bytes"
	"encoding/binary"
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
	bucketSize := filterParams.TotalElements / uint64(filterParams.BucketsCount)
	filters := make([]*bloom.BloomFilter, filterParams.BucketsCount)
	for i := uint32(0); i < filterParams.BucketsCount; i++ {
		filters[i] = bloom.NewWithEstimates(uint(bucketSize), filterParams.FalsePositives)
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
	return b.filters[b.filterParams.BucketID(data)]
}
