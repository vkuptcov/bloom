package bloom

import (
	"context"
	"io"
	"strconv"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/pkg/errors"
	"github.com/vkuptcov/bloom/redisclients"
)

type redisBloom struct {
	client      redisclients.RedisClient
	cachePrefix string

	filterParams FilterParams

	bitsCount           uint
	hashFunctionsNumber uint
}

// the wordSize of a bit set
const wordSize = uint64(64)

// first 3 words are reserved for BloomFilter header that is compatible
// with internal implementations of the in-memory filer
const dataOffset = wordSize * 3

func NewRedisBloom(
	redisClient redisclients.RedisClient,
	cachePrefix string,
	filterParams FilterParams,
) *redisBloom {
	bitsCount, hashFunctionsNumber := filterParams.EstimatedBucketParameters()
	return &redisBloom{
		client:              redisClient,
		cachePrefix:         cachePrefix,
		filterParams:        filterParams,
		bitsCount:           bitsCount,
		hashFunctionsNumber: hashFunctionsNumber,
	}
}

func (r *redisBloom) Init(ctx context.Context) error {
	pipeliner := r.client.Pipeliner(ctx)
	for bucketID := uint32(0); bucketID < r.filterParams.BucketsCount; bucketID++ {
		key := r.redisKeyByBucket(uint64(bucketID))
		pipeliner.BitField(
			key,
			// bits number for *bloom.BloomFilter
			"SET", "i64", "#0", int64(r.bitsCount),
			// hash functions count for *bloom.BloomFilter
			"SET", "i64", "#1", int64(r.hashFunctionsNumber),
			// bits number for *bitset.BitSet
			"SET", "i64", "#2", int64(r.bitsCount),
		)
		pipeliner.SetBits(key, dataOffset+((uint64(r.bitsCount)/wordSize)+1)*wordSize+1)
	}
	return errors.Wrap(pipeliner.Exec(), "Bloom filter buckets init error")
}

func (r *redisBloom) Add(ctx context.Context, data []byte) error {
	offsets := r.bitsOffset(data)
	key := r.redisKey(data)
	pipeliner := r.client.Pipeliner(ctx)
	pipeliner.SetBits(key, offsets...)
	pipeliner.Publish(r.cachePrefix, data)
	return errors.Wrap(pipeliner.Exec(), "filter update in Redis failed")
}

func (r *redisBloom) Test(ctx context.Context, data []byte) (bool, error) {
	offsets := r.bitsOffset(data)
	key := r.redisKey(data)
	return r.client.CheckBits(ctx, key, offsets...)
}

func (r *redisBloom) WriteTo(ctx context.Context, bucketID uint64, stream io.Writer) (int64, error) {
	b, gettingBytesErr := r.client.Get(ctx, r.redisKeyByBucket(bucketID))
	if gettingBytesErr != nil {
		return 0, errors.Wrapf(gettingBytesErr, "getting filter data failed for bucket %d", bucketID)
	}
	size, writeErr := stream.Write(b[0:(len(b) - 1)])
	return int64(size), errors.Wrapf(writeErr, "write filter data failed for bucket %d", bucketID)
}

func (r *redisBloom) bitsOffset(data []byte) []uint64 {
	locations := bloom.Locations(data, r.hashFunctionsNumber)
	for idx, l := range locations {
		locations[idx] = redisOffset(l % uint64(r.bitsCount))
	}
	return locations
}

func (r *redisBloom) redisKey(data []byte) string {
	return r.redisKeyByBucket(r.filterParams.BucketID(data))
}

func (r *redisBloom) redisKeyByBucket(bucketID uint64) string {
	return r.cachePrefix +
		"|" + strconv.FormatUint(uint64(r.bitsCount), 10) +
		"|" + strconv.FormatUint(uint64(r.hashFunctionsNumber), 10) +
		"|" + strconv.FormatUint(bucketID, 10)
}

func redisOffset(bitNum uint64) uint64 {
	wordNum := bitNum / wordSize
	wordShift := wordSize - (bitNum & (wordSize - 1)) - 1
	return dataOffset + wordNum*wordSize + wordShift
}
