package bloom

import (
	"context"
	"strconv"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

type RedisBloom struct {
	redis        redis.Cmdable
	cachePrefix  string
	bucketsCount uint32

	totalElements  uint64
	falsePositives float64

	bitsNumber          uint
	hashFunctionsNumber uint
}

func NewRedisBloom(
	redis redis.Cmdable,
	cachePrefix string,
	bucketsCount uint32,
	totalElements uint64,
	falsePositives float64,
) *RedisBloom {
	bitsNumber, hashFunctionsNumber := bloom.EstimateParameters(
		uint(totalElements)/uint(bucketsCount),
		falsePositives,
	)
	return &RedisBloom{
		redis:               redis,
		cachePrefix:         cachePrefix,
		bucketsCount:        bucketsCount,
		totalElements:       totalElements,
		falsePositives:      falsePositives,
		bitsNumber:          bitsNumber,
		hashFunctionsNumber: hashFunctionsNumber,
	}
}

func (r *RedisBloom) Init(ctx context.Context) error {
	pipeliner := r.redis.Pipeline()
	for bucketID := uint32(0); bucketID < r.bucketsCount; bucketID++ {
		key := r.redisKeyByBucket(uint64(bucketID))
		pipeliner.BitField(ctx, key, "SET", "u1", r.bitsNumber+1, 1)
	}
	_, err := pipeliner.Exec(ctx)
	return errors.Wrap(err, "Bloom filter buckets init error")
}

func (r *RedisBloom) Add(ctx context.Context, data []byte) error {
	offsets := r.bitsOffset(data)
	bitFieldArgs := make([]interface{}, 0, len(offsets)*4)
	for _, l := range offsets {
		offset := l % uint64(r.bitsNumber)
		bitFieldArgs = append(bitFieldArgs, "SET", "u1", offset, 1)
	}
	key := r.redisKey(data)
	sliceCmd := r.redis.BitField(ctx, key, bitFieldArgs...)
	return sliceCmd.Err()
}

func (r *RedisBloom) Test(ctx context.Context, data []byte) (bool, error) {
	offsets := r.bitsOffset(data)
	bitFieldArgs := make([]interface{}, 0, len(offsets)*3)
	for _, l := range offsets {
		offset := l % uint64(r.bitsNumber)
		bitFieldArgs = append(bitFieldArgs, "GET", "u1", offset)
	}
	key := r.redisKey(data)
	sliceCmds := r.redis.BitField(ctx, key, bitFieldArgs...)
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

func (r *RedisBloom) bitsOffset(data []byte) []uint64 {
	locations := bloom.Locations(data, r.hashFunctionsNumber)
	for idx, l := range locations {
		locations[idx] = l % uint64(r.bitsNumber)
	}
	return locations
}

func (r *RedisBloom) redisKey(data []byte) string {
	return r.redisKeyByBucket(bucketID(data, uint64(r.bucketsCount)))
}

func (r *RedisBloom) redisKeyByBucket(bucketID uint64) string {
	return r.cachePrefix + "|" + strconv.FormatUint(bucketID, 10)
}
