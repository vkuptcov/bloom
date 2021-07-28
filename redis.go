package bloom

import (
	"context"
	"strconv"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/go-redis/redis/v8"
)

type RedisBloom struct {
	redis        redis.Cmdable
	cachePrefix  string
	bucketsCount uint32

	totalElements  uint64
	falsePositives float64
}

func NewRedisBloom(
	redis redis.Cmdable,
	cachePrefix string,
	bucketsCount uint32,
	totalElements uint64,
	falsePositives float64,
) *RedisBloom {
	return &RedisBloom{
		redis:          redis,
		cachePrefix:    cachePrefix,
		bucketsCount:   bucketsCount,
		totalElements:  totalElements,
		falsePositives: falsePositives,
	}
}

func (r *RedisBloom) Init() {
	// @todo allocate necessary sets
}

func (r *RedisBloom) Add(ctx context.Context, data []byte) error {
	bitsNumber, hashFunctionsNumber := bloom.EstimateParameters(
		uint(r.totalElements)/uint(r.bucketsCount),
		r.falsePositives,
	)
	locations := bloom.Locations(data, hashFunctionsNumber)
	bitFieldArgs := make([]interface{}, 0, len(locations)*4)
	for _, l := range locations {
		offset := l % uint64(bitsNumber)
		bitFieldArgs = append(bitFieldArgs, "SET", "u1", offset, 1)
	}
	key := r.cachePrefix + "|" + strconv.Itoa(int(bucketID(data, uint64(r.bucketsCount))))
	sliceCmd := r.redis.BitField(ctx, key, bitFieldArgs...)
	return sliceCmd.Err()
}

func (r *RedisBloom) Test(ctx context.Context, data []byte) (bool, error) {
	bitsNumber, hashFunctionsNumber := bloom.EstimateParameters(
		uint(r.totalElements)/uint(r.bucketsCount),
		r.falsePositives,
	)
	locations := bloom.Locations(data, hashFunctionsNumber)
	bitFieldArgs := make([]interface{}, 0, len(locations)*3)
	for _, l := range locations {
		offset := l % uint64(bitsNumber)
		bitFieldArgs = append(bitFieldArgs, "GET", "u1", offset)
	}
	key := r.cachePrefix + "|" + strconv.Itoa(int(bucketID(data, uint64(r.bucketsCount))))
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
