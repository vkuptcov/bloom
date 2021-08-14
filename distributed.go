package bloom

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/vkuptcov/bloom/redisclients"
)

type DistributedFilter struct {
	redisClient                   redisclients.RedisClient
	FilterParams                  FilterParams
	CachePrefix                   string
	inMemory                      *InMemoryBlooms
	redisBloom                    *RedisBloom
	testInterceptor               testInterceptor
	fillInStrategies              []DataLoader
	checkRedisConsistencyInterval time.Duration
	initializedBuckets            *initializedBuckets
	logger                        Logger
	isListensToUpdate             *atomic.Value
}

func NewDistributedFilter(
	redisClient redisclients.RedisClient,
	cachePrefix string,
	filterParams FilterParams,
	strategies ...DataLoader,
) *DistributedFilter {
	f := &DistributedFilter{
		redisClient:     redisClient,
		FilterParams:    filterParams,
		CachePrefix:     cachePrefix,
		inMemory:        NewInMemory(filterParams),
		redisBloom:      NewRedisBloom(redisClient, cachePrefix, filterParams),
		testInterceptor: defaultNoOp,
		fillInStrategies: append([]DataLoader{
			RedisStateCheck,
			BulkLoaderFromRedis,
		}, strategies...),
		checkRedisConsistencyInterval: 5 * time.Minute,
		initializedBuckets:            &initializedBuckets{},
		logger:                        StdLogger(nil),
		isListensToUpdate:             &atomic.Value{},
	}
	f.isListensToUpdate.Store(false)
	return f
}

func (df *DistributedFilter) setTestInterceptor(testInterceptor testInterceptor) {
	df.testInterceptor = testInterceptor
}

func (df *DistributedFilter) Init(ctx context.Context) error {
	listensForUpdate := df.isListensToUpdate.Load().(bool)
	if !listensForUpdate {
		pubSub, listenErr := df.redisClient.Listen(ctx, df.redisBloom.cachePrefix)
		if listenErr != nil {
			return errors.Wrap(listenErr, "redis filter subscription failed")
		}
		df.isListensToUpdate.Store(true)
		go df.listenForChanges(pubSub)
	}

	initRedisFilterErr := df.redisBloom.Init(ctx)
	if initRedisFilterErr != nil {
		return errors.Wrap(initRedisFilterErr, "redis filter initialization failed")
	}
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

func (df *DistributedFilter) GenerateBucketsMap() (map[uint32][]byte, error) {
	var batchErr *multierror.Error
	m := make(map[uint32][]byte, df.FilterParams.BucketsCount)
	for bucketID := uint32(0); bucketID < df.FilterParams.BucketsCount; bucketID++ {
		if !df.initializedBuckets.isInitialized(bucketID) {
			// @todo consider adding an error
			continue
		}
		var buf bytes.Buffer
		_, writeErr := df.inMemory.WriteTo(uint64(bucketID), &buf)
		if writeErr != nil {
			batchErr = multierror.Append(batchErr, errors.Wrapf(writeErr, "bucket %d bytes generation failed", bucketID))
		} else {
			m[bucketID] = buf.Bytes()
		}
	}
	return m, batchErr.ErrorOrNil()
}

func (df *DistributedFilter) WriteTo(bucketID uint64, stream io.Writer) (int64, error) {
	return df.inMemory.WriteTo(bucketID, stream)
}

func (df *DistributedFilter) listenForChanges(pubSub <-chan string) {
	for message := range pubSub {
		df.inMemory.Add([]byte(message))
	}
}

func (df *DistributedFilter) loadDataFromSources(ctx context.Context) error {
	df.testInterceptor.interfere("before-in-memory-init")
	for _, fs := range df.fillInStrategies {
		results, dataLoaderErr := fs(ctx, df)
		// if we have at least one bucket loaded it's better than nothing
		handleResultErr := df.handleDataLoadResults(ctx, results)
		if handleResultErr != nil || dataLoaderErr != nil {
			return multierror.Append(dataLoaderErr, handleResultErr)
		}
		if !results.NeedRunNextLoader {
			break
		}
	}
	return nil
}

func (df *DistributedFilter) handleDataLoadResults(ctx context.Context, results DataLoaderResults) error {
	errorsBatch := df.applySourcesToBuckets(ctx, results)
	errorsBatch = df.tryFinalizeFilterState(ctx, results, errorsBatch)
	return errorsBatch.ErrorOrNil()
}

func (df *DistributedFilter) applySourcesToBuckets(ctx context.Context, results DataLoaderResults) *multierror.Error {
	var errorsBatch *multierror.Error
	if len(results.SourcesPerBucket) > 0 {
		for bucketID, bucketContent := range results.SourcesPerBucket {
			if restoreErr := df.inMemory.AddFrom(bucketID, bytes.NewReader(bucketContent)); restoreErr != nil {
				errorsBatch = multierror.Append(errorsBatch, errors.Wrap(restoreErr, "in-memory filter restore failed"))
				continue
			}
			if results.DumpStateInRedis {
				if dumpErr := df.dumpStateInRedis(ctx, bucketID, results.FinalizeFilter); dumpErr != nil {
					errorsBatch = multierror.Append(errorsBatch, errors.Wrap(dumpErr, "redis dump filter failed"))
				}
			}
		}
	}
	return errorsBatch
}

func (df *DistributedFilter) tryFinalizeFilterState(ctx context.Context, results DataLoaderResults, errorsBatch *multierror.Error) *multierror.Error {
	if results.FinalizeFilter && errorsBatch.ErrorOrNil() == nil {
		for bucketID := uint32(0); bucketID < df.FilterParams.BucketsCount; bucketID++ {
			df.initializedBuckets.initialize(bucketID)
			initializationErr := df.redisBloom.initializeFilter(ctx, uint64(bucketID))
			if initializationErr != nil {
				errorsBatch = multierror.Append(errorsBatch, errors.Wrap(initializationErr, "redis initialization filter failed"))
			}
		}
	}
	return errorsBatch
}

func (df *DistributedFilter) dumpStateInRedis(ctx context.Context, bucketID uint64, addFinalBit bool) error {
	var buf bytes.Buffer
	if _, writeBufferErr := df.inMemory.WriteTo(bucketID, &buf); writeBufferErr != nil {
		return errors.Wrapf(writeBufferErr, "in-memory filter serialzation failed for bucket %d", bucketID)
	}
	return df.redisBloom.MergeWith(ctx, bucketID, addFinalBit, &buf)
}
