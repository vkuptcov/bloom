package bloom

import (
	"bufio"
	"bytes"
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

type distributedFilter struct {
	redisClient redis.Cmdable
	inMemory    *inMemoryBlooms
	redisBloom  *redisBloom
}

func NewDistributedFilter(
	redisClient redis.Cmdable,
	cachePrefix string,
	filterParams FilterParams,
) *distributedFilter {
	f := &distributedFilter{
		redisClient: redisClient,
		inMemory:    NewInMemory(filterParams),
		redisBloom:  NewRedisBloom(redisClient, cachePrefix, filterParams),
	}
	return f
}

func (df *distributedFilter) Init(ctx context.Context) error {
	initRedisFilterErr := df.redisBloom.Init(ctx)
	if initRedisFilterErr != nil {
		return errors.Wrap(initRedisFilterErr, "redis filter initialization failed")
	}
	return df.initInMemoryFilter(ctx)
}

func (df *distributedFilter) Add(ctx context.Context, data []byte) error {
	if addIntoRedisErr := df.redisBloom.Add(ctx, data); addIntoRedisErr != nil {
		return addIntoRedisErr
	}
	df.inMemory.Add(data)
	return nil
}

func (df *distributedFilter) Test(data []byte) bool {
	return df.inMemory.Test(data)
}

func (df *distributedFilter) initInMemoryFilter(ctx context.Context) error {
	for bucketID := uint64(0); bucketID < uint64(df.inMemory.filterParams.BucketsCount); bucketID++ {
		var redisFilterBuf bytes.Buffer
		writer := bufio.NewWriter(&redisFilterBuf)
		if _, redisBloomWriteErr := df.redisBloom.WriteTo(ctx, bucketID, writer); redisBloomWriteErr != nil {
			return errors.Wrap(redisBloomWriteErr, "bloom filter load from Redis failed")
		}

		if flushErr := writer.Flush(); flushErr != nil {
			return errors.Wrap(flushErr, "bloom filter flush failed")
		}

		if restoreErr := df.inMemory.Restore(bucketID, bytes.NewReader(redisFilterBuf.Bytes())); restoreErr != nil {
			return errors.Wrap(restoreErr, "in-memory filter restore failed")
		}
	}
	return nil
}
