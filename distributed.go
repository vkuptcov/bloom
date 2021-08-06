package bloom

import (
	"bufio"
	"bytes"
	"context"

	"github.com/pkg/errors"
	"github.com/vkuptcov/bloom/redisclients"
)

type DistributedFilter struct {
	redisClient     redisclients.RedisClient
	inMemory        *inMemoryBlooms
	redisBloom      *redisBloom
	testInterceptor testInterceptor
}

func NewDistributedFilter(
	redisClient redisclients.RedisClient,
	cachePrefix string,
	filterParams FilterParams,
) *DistributedFilter {
	f := &DistributedFilter{
		redisClient:     redisClient,
		inMemory:        NewInMemory(filterParams),
		redisBloom:      NewRedisBloom(redisClient, cachePrefix, filterParams),
		testInterceptor: defaultNoOp,
	}
	return f
}

func (df *DistributedFilter) setTestInterceptor(testInterceptor testInterceptor) {
	df.testInterceptor = testInterceptor
}

func (df *DistributedFilter) Init(ctx context.Context) error {
	pubSub, listenErr := df.redisClient.Listen(ctx, df.redisBloom.cachePrefix)
	if listenErr != nil {
		return errors.Wrap(listenErr, "redis filter subscription failed")
	}

	initRedisFilterErr := df.redisBloom.Init(ctx)
	if initRedisFilterErr != nil {
		return errors.Wrap(initRedisFilterErr, "redis filter initialization failed")
	}
	go df.listenForChanges(pubSub)
	return df.initInMemoryFilter(ctx)
}

func (df *DistributedFilter) Add(ctx context.Context, data []byte) error {
	if addIntoRedisErr := df.redisBloom.Add(ctx, data); addIntoRedisErr != nil {
		return addIntoRedisErr
	}
	df.inMemory.Add(data)
	return nil
}

func (df *DistributedFilter) AddString(ctx context.Context, data string) error {
	return df.Add(ctx, []byte(data))
}

func (df *DistributedFilter) AddUint16(ctx context.Context, i uint16) error {
	return df.Add(ctx, uint16ToByte(i))
}

func (df *DistributedFilter) AddUint32(ctx context.Context, i uint32) error {
	return df.Add(ctx, uint32ToByte(i))
}

func (df *DistributedFilter) AddUint64(ctx context.Context, i uint64) error {
	return df.Add(ctx, uint64ToByte(i))
}

func (df *DistributedFilter) Test(data []byte) bool {
	return df.inMemory.Test(data)
}

func (df *DistributedFilter) TestString(data string) bool {
	return df.Test([]byte(data))
}

func (df *DistributedFilter) TestUint16(i uint16) bool {
	return df.Test(uint16ToByte(i))
}

func (df *DistributedFilter) TestUint32(i uint32) bool {
	return df.Test(uint32ToByte(i))
}

func (df *DistributedFilter) TestUint64(i uint64) bool {
	return df.Test(uint64ToByte(i))
}

func (df *DistributedFilter) initInMemoryFilter(ctx context.Context) error {
	df.testInterceptor.interfere("before-in-memory-init")
	for bucketID := uint64(0); bucketID < uint64(df.inMemory.filterParams.BucketsCount); bucketID++ {
		var redisFilterBuf bytes.Buffer
		writer := bufio.NewWriter(&redisFilterBuf)
		if _, redisBloomWriteErr := df.redisBloom.WriteTo(ctx, bucketID, writer); redisBloomWriteErr != nil {
			return errors.Wrap(redisBloomWriteErr, "bloom filter load from Redis failed")
		}

		if flushErr := writer.Flush(); flushErr != nil {
			return errors.Wrap(flushErr, "bloom filter flush failed")
		}

		if restoreErr := df.inMemory.AddFrom(bucketID, bytes.NewReader(redisFilterBuf.Bytes())); restoreErr != nil {
			return errors.Wrap(restoreErr, "in-memory filter restore failed")
		}
	}
	return nil
}

func (df *DistributedFilter) listenForChanges(pubSub <-chan *redisclients.Message) {
	for message := range pubSub {
		if message.Channel == df.redisBloom.cachePrefix {
			df.inMemory.Add([]byte(message.Payload))
		}
	}
}
