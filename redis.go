package bloom

import (
	"context"
	"encoding/binary"
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
		pipeliner.SetBits(key, ((uint64(r.bitsCount)/wordSize)+1)*wordSize+1)
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
	header := []uint{
		// bits number for *bloom.BloomFilter
		r.bitsCount,
		// hash functions count for *bloom.BloomFilter
		r.hashFunctionsNumber,
		// bits number for *bitset.BitSet
		r.bitsCount,
	}
	for _, h := range header {
		writeHeaderErr := binary.Write(stream, binary.BigEndian, uint64(h))
		if writeHeaderErr != nil {
			return 0, errors.Wrapf(writeHeaderErr, "header write error for bucket %d", bucketID)
		}
	}
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
	return r.cachePrefix + "|" + strconv.FormatUint(bucketID, 10)
}

func redisOffset(bitNum uint64) uint64 {
	wordNum := bitNum / wordSize
	wordShift := wordSize - (bitNum & (wordSize - 1)) - 1
	return wordNum*wordSize + wordShift
}
