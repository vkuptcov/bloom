package bloom

import (
	"bufio"
	"bytes"
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

type distributedFilter struct {
	redisClient     redis.UniversalClient
	inMemory        *inMemoryBlooms
	redisBloom      *redisBloom
	testInterceptor testInterceptor
}

func NewDistributedFilter(
	redisClient redis.UniversalClient,
	cachePrefix string,
	filterParams FilterParams,
) *distributedFilter {
	f := &distributedFilter{
		redisClient:     redisClient,
		inMemory:        NewInMemory(filterParams),
		redisBloom:      NewRedisBloom(NewGoRedisClient(redisClient), cachePrefix, filterParams),
		testInterceptor: defaultNoOp,
	}
	return f
}

func (df *distributedFilter) setTestInterceptor(testInterceptor testInterceptor) {
	df.testInterceptor = testInterceptor
}

func (df *distributedFilter) Init(ctx context.Context) error {
	pubSub := df.redisClient.Subscribe(ctx, df.redisBloom.cachePrefix)
	if _, receiveErr := pubSub.Receive(ctx); receiveErr != nil {
		return errors.Wrap(receiveErr, "redis filter subscription failed")
	}

	initRedisFilterErr := df.redisBloom.Init(ctx)
	if initRedisFilterErr != nil {
		return errors.Wrap(initRedisFilterErr, "redis filter initialization failed")
	}
	go df.listenForChanges(pubSub)
	return df.initInMemoryFilter(ctx)
}

func (df *distributedFilter) Add(ctx context.Context, data []byte) error {
	if addIntoRedisErr := df.redisBloom.Add(ctx, data); addIntoRedisErr != nil {
		return addIntoRedisErr
	}
	df.inMemory.Add(data)
	return nil
}

func (df *distributedFilter) AddString(ctx context.Context, data string) error {
	return df.Add(ctx, []byte(data))
}

func (df *distributedFilter) AddUint16(ctx context.Context, i uint16) error {
	return df.Add(ctx, uint16ToByte(i))
}

func (df *distributedFilter) AddUint32(ctx context.Context, i uint32) error {
	return df.Add(ctx, uint32ToByte(i))
}

func (df *distributedFilter) AddUint64(ctx context.Context, i uint64) error {
	return df.Add(ctx, uint64ToByte(i))
}

func (df *distributedFilter) Test(data []byte) bool {
	return df.inMemory.Test(data)
}

func (df *distributedFilter) TestString(data string) bool {
	return df.Test([]byte(data))
}

func (df *distributedFilter) TestUint16(i uint16) bool {
	return df.Test(uint16ToByte(i))
}

func (df *distributedFilter) TestUint32(i uint32) bool {
	return df.Test(uint32ToByte(i))
}

func (df *distributedFilter) TestUint64(i uint64) bool {
	return df.Test(uint64ToByte(i))
}

func (df *distributedFilter) initInMemoryFilter(ctx context.Context) error {
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

func (df *distributedFilter) listenForChanges(pubSub *redis.PubSub) {
	for message := range pubSub.Channel() {
		if message.Channel == df.redisBloom.cachePrefix {
			df.inMemory.Add([]byte(message.Payload))
		}
	}
}
