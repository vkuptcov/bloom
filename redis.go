package bloom

import (
	"context"
	"encoding/binary"
	"io"
	"strconv"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

type redisBloom struct {
	redisClient redis.Cmdable
	cachePrefix string

	filterParams FilterParams

	bitsCount           uint
	hashFunctionsNumber uint
}

// the wordSize of a bit set
const wordSize = uint64(64)

func NewRedisBloom(
	redisClient redis.Cmdable,
	cachePrefix string,
	filterParams FilterParams,
) *redisBloom {
	bitsCount, hashFunctionsNumber := filterParams.EstimatedBucketParameters()
	return &redisBloom{
		redisClient:         redisClient,
		cachePrefix:         cachePrefix,
		filterParams:        filterParams,
		bitsCount:           bitsCount,
		hashFunctionsNumber: hashFunctionsNumber,
	}
}

func (r *redisBloom) Init(ctx context.Context) error {
	pipeliner := r.redisClient.Pipeline()
	for bucketID := uint32(0); bucketID < r.filterParams.BucketsCount; bucketID++ {
		key := r.redisKeyByBucket(uint64(bucketID))
		pipeliner.BitField(ctx, key, "SET", "u1", (uint64(r.bitsCount)/wordSize)*wordSize+wordSize+1, 1)
	}
	_, err := pipeliner.Exec(ctx)
	return errors.Wrap(err, "Bloom filter buckets init error")
}

func (r *redisBloom) Add(ctx context.Context, data []byte) error {
	offsets := r.bitsOffset(data)
	bitFieldArgs := make([]interface{}, 0, len(offsets)*4)
	for _, l := range offsets {
		offset := l % uint64(r.bitsCount)
		bitFieldArgs = append(bitFieldArgs, "SET", "u1", redisOffset(offset), 1)
	}
	key := r.redisKey(data)
	_, err := r.redisClient.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		_ = pipeliner.BitField(ctx, key, bitFieldArgs...)
		pipeliner.Publish(ctx, r.cachePrefix, data)
		return nil
	})
	return err
}

func (r *redisBloom) Test(ctx context.Context, data []byte) (bool, error) {
	offsets := r.bitsOffset(data)
	bitFieldArgs := make([]interface{}, 0, len(offsets)*3)
	for _, l := range offsets {
		offset := l % uint64(r.bitsCount)
		bitFieldArgs = append(bitFieldArgs, "GET", "u1", redisOffset(offset))
	}
	key := r.redisKey(data)
	sliceCmds := r.redisClient.BitField(ctx, key, bitFieldArgs...)
	res, err := sliceCmds.Result()
	if err != nil {
		return false, err
	}
	for _, s := range res {
		if s == 0 {
			return false, nil
		}
	}
	return true, nil
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
	stringCmd := r.redisClient.Get(ctx, r.redisKeyByBucket(bucketID))
	b, gettingBytesErr := stringCmd.Bytes()
	if gettingBytesErr != nil {
		return 0, errors.Wrapf(gettingBytesErr, "getting filter data failed for bucket %d", bucketID)
	}
	size, writeErr := stream.Write(b[0:(len(b) - 1)])
	return int64(size), errors.Wrapf(writeErr, "write filter data failed for bucket %d", bucketID)
}

func (r *redisBloom) bitsOffset(data []byte) []uint64 {
	locations := bloom.Locations(data, r.hashFunctionsNumber)
	for idx, l := range locations {
		locations[idx] = l % uint64(r.bitsCount)
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
