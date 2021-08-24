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
		hooks:                         NewHooks(),
	}
	f.isListensToUpdate.Store(false)
	return f
}

func (df *DistributedFilter) SetHooks(hooks *Hooks) {
	df.hooks = hooks
}

func (df *DistributedFilter) Init(ctx context.Context) (initErr error) {
	defer func() {
		df.hooks.After(GlobalInit, initErr)
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
		df.hooks.Before(LoadDataFromSource, fs.Name())
		results, dataLoaderErr := fs.Load(ctx, df)
		if results.Name == "" {
			results.Name = fs.Name()
		}
		// if we have at least one bucket loaded it's better than nothing
		handleResultErr := df.handleDataLoadResults(ctx, fs.Name(), results)
		if handleResultErr != nil || dataLoaderErr != nil {
			err := multierror.Append(dataLoaderErr, handleResultErr)
			df.hooks.AfterFail(LoadDataFromSource, err.ErrorOrNil(), fs.Name(), results)
			df.hooks.AfterFail(LoadData, err.ErrorOrNil())
			return err
		}
		df.hooks.AfterSuccess(LoadDataFromSource, fs.Name(), results)
		if !results.NeedRunNextLoader {
			break
		}
	}
	df.hooks.AfterSuccess(LoadData)
	return nil
}

func (df *DistributedFilter) handleDataLoadResults(ctx context.Context, dataSourceName string, results DataLoaderResults) error {
	errorsBatch := df.applySourcesToBuckets(ctx, dataSourceName, results)
	errorsBatch = df.tryFinalizeFilterState(ctx, dataSourceName, results, errorsBatch)
	return errorsBatch.ErrorOrNil()
}

// applySourcesToBuckets adds the data from the data loaders into in-memory filters
// and it also optionally dumps data in Redis
func (df *DistributedFilter) applySourcesToBuckets(ctx context.Context, dataSourceName string, results DataLoaderResults) *multierror.Error {
	var errorsBatch *multierror.Error
	if len(results.SourcesPerBucket) > 0 {
		df.hooks.Before(ApplySources, dataSourceName, results)
		for bucketID, bucketContent := range results.SourcesPerBucket {
			df.hooks.Before(ApplySourceForBucket, bucketID, dataSourceName, results)
			if restoreErr := df.inMemory.AddFrom(bucketID, bytes.NewReader(bucketContent)); restoreErr != nil {
				restoreErr = errors.Wrapf(restoreErr, "in-memory filter restore failed for bucket %d", bucketID)
				df.hooks.AfterFail(ApplySourceForBucket, restoreErr, bucketID, dataSourceName, results)
				errorsBatch = multierror.Append(errorsBatch, restoreErr)
				continue
			}
			df.hooks.AfterSuccess(ApplySourceForBucket, bucketID, dataSourceName, results)
			if results.DumpStateInRedis {
				df.hooks.Before(DumpStateInRedisForBucket, bucketID, dataSourceName, results)
				dumpErr := df.dumpStateInRedis(ctx, bucketID, results.FinalizeFilter)
				if dumpErr != nil {
					dumpErr = errors.Wrapf(dumpErr, "redis dump filter failed for bucket %d", bucketID)
					errorsBatch = multierror.Append(errorsBatch, dumpErr)
				}
				df.hooks.After(DumpStateInRedisForBucket, dumpErr, bucketID, dataSourceName, results)
			}
		}
		df.hooks.After(ApplySources, errorsBatch.ErrorOrNil(), dataSourceName, results)
	}
	return errorsBatch
}

func (df *DistributedFilter) tryFinalizeFilterState(ctx context.Context, dataSourceName string, results DataLoaderResults, errorsBatch *multierror.Error) *multierror.Error {
	if results.FinalizeFilter && errorsBatch.ErrorOrNil() == nil {
		df.hooks.Before(FinalizeFilters, dataSourceName, results)
		for bucketID := uint32(0); bucketID < df.FilterParams.BucketsCount; bucketID++ {
			df.hooks.Before(FinalizeFilterForBucket, bucketID, dataSourceName, results)
			df.initializedBuckets.initialize(bucketID)
			initializationErr := df.redisBloom.initializeFilter(ctx, uint64(bucketID))
			if initializationErr != nil {
				initializationErr = errors.Wrapf(initializationErr, "redis initialization filter failed for bucket %d", bucketID)
				errorsBatch = multierror.Append(errorsBatch, initializationErr)
			}
			df.hooks.After(FinalizeFilterForBucket, initializationErr, bucketID, dataSourceName, results)
		}
		df.hooks.After(FinalizeFilters, errorsBatch.ErrorOrNil(), dataSourceName, results)
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
