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
	fillInStrategies              []DataLoader
	checkRedisConsistencyInterval time.Duration
	initializedBuckets            *initializedBuckets
	logger                        Logger
	isListensToUpdate             *atomic.Value
	hooks                         *Hooks
}

func NewDistributedFilter(
	redisClient redisclients.RedisClient,
	cachePrefix string,
	filterParams FilterParams,
	strategies ...DataLoader,
) *DistributedFilter {
	f := &DistributedFilter{
		redisClient:  redisClient,
		FilterParams: filterParams,
		CachePrefix:  cachePrefix,
		inMemory:     NewInMemory(filterParams),
		redisBloom:   NewRedisBloom(redisClient, cachePrefix, filterParams),
		fillInStrategies: append([]DataLoader{
			RedisStateCheck,
			BulkLoaderFromRedis,
		}, strategies...),
		checkRedisConsistencyInterval: 5 * time.Minute,
		initializedBuckets:            &initializedBuckets{},
		logger:                        StdLogger(nil),
		isListensToUpdate:             &atomic.Value{},
		hooks:                         &Hooks{},
	}
	f.isListensToUpdate.Store(false)
	return f
}

func (df *DistributedFilter) SetHooks(hooks *Hooks) {
	df.hooks = hooks
}

func (df *DistributedFilter) Init(ctx context.Context) (initErr error) {
	defer func() {
		if initErr != nil {
			df.hooks.AfterFail(GlobalInit, initErr)
		} else {
			df.hooks.AfterSuccess(GlobalInit)
		}
	}()
	df.hooks.Before(GlobalInit)
	listensForUpdate := df.isListensToUpdate.Load().(bool)
	if !listensForUpdate {
		df.hooks.Before(StartUpdatesListening)
		pubSub, listenErr := df.redisClient.Listen(ctx, df.redisBloom.cachePrefix)
		if listenErr != nil {
			listenErr = errors.Wrap(listenErr, "redis filter subscription failed")
			df.hooks.AfterFail(StartUpdatesListening, listenErr)
			return listenErr
		}
		df.isListensToUpdate.Store(true)
		go df.listenForChanges(pubSub)
	}

	df.hooks.Before(RedisInit)
	initRedisFilterErr := df.redisBloom.Init(ctx)
	if initRedisFilterErr != nil {
		initRedisFilterErr = errors.Wrap(initRedisFilterErr, "redis filter initialization failed")
		df.hooks.AfterFail(RedisInit, initRedisFilterErr)
		return initRedisFilterErr
	}
	df.hooks.AfterSuccess(RedisInit)
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
	df.hooks.Before(GenerateBuckets)
	var batchErr *multierror.Error
	m := make(map[uint32][]byte, df.FilterParams.BucketsCount)
	for bucketID := uint32(0); bucketID < df.FilterParams.BucketsCount; bucketID++ {
		df.hooks.Before(GenerateParticularBucket, bucketID)
		if !df.initializedBuckets.isInitialized(bucketID) {
			// @todo consider adding an error
			df.hooks.AfterSuccess(GenerateParticularBucket, bucketID, "bucket is not initialized in memory")
			continue
		}
		var buf bytes.Buffer
		_, writeErr := df.inMemory.WriteTo(uint64(bucketID), &buf)
		if writeErr != nil {
			writeErr = errors.Wrapf(writeErr, "bucket %d bytes generation failed", bucketID)
			df.hooks.AfterFail(GenerateParticularBucket, writeErr, bucketID)
			batchErr = multierror.Append(batchErr, writeErr)
		} else {
			m[bucketID] = buf.Bytes()
			df.hooks.AfterSuccess(GenerateParticularBucket, bucketID, "bucket is initialized in memory")
		}
	}
	df.hooks.After(GenerateBuckets, batchErr.ErrorOrNil())
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
	df.hooks.Before(LoadData)
	for _, fs := range df.fillInStrategies {
		results, dataLoaderErr := fs(ctx, df)
		// if we have at least one bucket loaded it's better than nothing
		handleResultErr := df.handleDataLoadResults(ctx, results)
		if handleResultErr != nil || dataLoaderErr != nil {
			err := multierror.Append(dataLoaderErr, handleResultErr)
			df.hooks.AfterFail(LoadData, err)
			return err
		}
		if !results.NeedRunNextLoader {
			break
		}
	}
	df.hooks.AfterSuccess(LoadData)
	return nil
}

func (df *DistributedFilter) handleDataLoadResults(ctx context.Context, results DataLoaderResults) error {
	df.hooks.Before(ApplySources)
	errorsBatch := df.applySourcesToBuckets(ctx, results)
	df.hooks.After(ApplySources, errorsBatch.ErrorOrNil())
	df.hooks.Before(FinalizeFilter)
	errorsBatch = df.tryFinalizeFilterState(ctx, results, errorsBatch)
	df.hooks.After(FinalizeFilter, errorsBatch.ErrorOrNil())
	return errorsBatch.ErrorOrNil()
}

// applySourcesToBuckets adds the data from the data loaders into in-memory filters
// and it also optionally dumps data in Redis
func (df *DistributedFilter) applySourcesToBuckets(ctx context.Context, results DataLoaderResults) *multierror.Error {
	var errorsBatch *multierror.Error
	if len(results.SourcesPerBucket) > 0 {
		for bucketID, bucketContent := range results.SourcesPerBucket {
			df.hooks.Before(ApplyParticularBucketSource, bucketID)
			if restoreErr := df.inMemory.AddFrom(bucketID, bytes.NewReader(bucketContent)); restoreErr != nil {
				restoreErr = errors.Wrapf(restoreErr, "in-memory filter restore failed for bucket %d", bucketID)
				df.hooks.AfterFail(ApplyParticularBucketSource, restoreErr, bucketID)
				errorsBatch = multierror.Append(errorsBatch, restoreErr)
				continue
			}
			df.hooks.AfterSuccess(ApplyParticularBucketSource, bucketID)
			if results.DumpStateInRedis {
				df.hooks.Before(DumpStateInRedis, bucketID)
				dumpErr := df.dumpStateInRedis(ctx, bucketID, results.FinalizeFilter)
				if dumpErr != nil {
					dumpErr = errors.Wrapf(dumpErr, "redis dump filter failed for bucket %d", bucketID)
					errorsBatch = multierror.Append(errorsBatch, dumpErr)
				}
				df.hooks.After(DumpStateInRedis, dumpErr, bucketID)
			}
		}
	}
	return errorsBatch
}

func (df *DistributedFilter) tryFinalizeFilterState(ctx context.Context, results DataLoaderResults, errorsBatch *multierror.Error) *multierror.Error {
	if results.FinalizeFilter && errorsBatch.ErrorOrNil() == nil {
		for bucketID := uint32(0); bucketID < df.FilterParams.BucketsCount; bucketID++ {
			df.hooks.Before(FinalizeParticularBucketFilter, bucketID)
			df.initializedBuckets.initialize(bucketID)
			initializationErr := df.redisBloom.initializeFilter(ctx, uint64(bucketID))
			if initializationErr != nil {
				initializationErr = errors.Wrapf(initializationErr, "redis initialization filter failed for bucket %d", bucketID)
				errorsBatch = multierror.Append(errorsBatch, initializationErr)
			}
			df.hooks.After(FinalizeParticularBucketFilter, initializationErr, bucketID)
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
