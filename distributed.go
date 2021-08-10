package bloom

import (
	"bytes"
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/vkuptcov/bloom/redisclients"
)

type DistributedFilter struct {
	redisClient                   redisclients.RedisClient
	inMemory                      *InMemoryBlooms
	redisBloom                    *RedisBloom
	testInterceptor               testInterceptor
	fillInStrategies              []BulkDataLoader
	checkRedisConsistencyInterval time.Duration
	logger                        Logger
	initializationFinished        bool
}

func NewDistributedFilter(
	redisClient redisclients.RedisClient,
	cachePrefix string,
	filterParams FilterParams,
	strategies ...BulkDataLoader,
) *DistributedFilter {
	f := &DistributedFilter{
		redisClient:     redisClient,
		inMemory:        NewInMemory(filterParams),
		redisBloom:      NewRedisBloom(redisClient, cachePrefix, filterParams),
		testInterceptor: defaultNoOp,
		fillInStrategies: append([]BulkDataLoader{
			&BulkLoaderFromRedis{},
		}, strategies...),
		checkRedisConsistencyInterval: 5 * time.Minute,
		logger:                        StdLogger(nil),
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
	return df.loadDataFromSources(ctx)
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

func (df *DistributedFilter) listenForChanges(pubSub <-chan string) {
	for message := range pubSub {
		df.inMemory.Add([]byte(message))
	}
}

func (df *DistributedFilter) loadDataFromSources(ctx context.Context) error {
	df.testInterceptor.interfere("before-in-memory-init")
	for _, fs := range df.fillInStrategies {
		sources, fillErr := fs.Sources(ctx, df)
		if fillErr != nil {
			return errors.Wrap(fillErr, "in-memory filter initialization failed")
		}
		for bucketID, bucketContent := range sources {
			if restoreErr := df.inMemory.AddFrom(bucketID, bytes.NewReader(bucketContent)); restoreErr != nil {
				return errors.Wrap(restoreErr, "in-memory filter restore failed")
			}
			if fs.DumpStateInRedis() {
				if dumpErr := df.dumpStateInRedis(ctx, bucketID); dumpErr != nil {
					return errors.Wrap(dumpErr, "redis dump filter failed")
				}
			}
		}
	}
	df.initializationFinished = true
	return nil
}

func (df *DistributedFilter) dumpStateInRedis(ctx context.Context, bucketID uint64) error {
	var buf bytes.Buffer
	if _, writeBufferErr := df.inMemory.WriteTo(bucketID, &buf); writeBufferErr != nil {
		return errors.Wrapf(writeBufferErr, "in-memory filter serialzation failed for bucket %d", bucketID)
	}
	return df.redisBloom.MergeWith(ctx, bucketID, &buf)
}
