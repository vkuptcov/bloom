package bloom

import (
	"bufio"
	"bytes"
	"context"
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

var UnsupportedDataTypeErr = errors.New("unsupported data type")

type DataLoaderResults struct {
	Name              string
	SourcesPerBucket  map[int][]byte
	DumpStateInRedis  bool
	NeedRunNextLoader bool
	FinalizeFilter    bool
}

func (r DataLoaderResults) String() string {
	return fmt.Sprintf(
		"DataLoaderResults(Name: %s, DumpStateInRedisForBucket: %t, NeedRunNextLoader: %t, FinalizeFilters: %t, len(SourcesPerBucket): %d)",
		r.Name,
		r.DumpStateInRedis,
		r.NeedRunNextLoader,
		r.FinalizeFilter,
		len(r.SourcesPerBucket),
	)
}

func DefaultResults() DataLoaderResults {
	return DataLoaderResults{
		SourcesPerBucket: map[int][]byte{},
	}
}

type dataLoaderImpl struct {
	name   string
	loader func(ctx context.Context, df *DistributedFilter) (DataLoaderResults, error)
}

func (d *dataLoaderImpl) Name() string {
	return d.name
}

func (d *dataLoaderImpl) Load(ctx context.Context, df *DistributedFilter) (DataLoaderResults, error) {
	return d.loader(ctx, df)
}

func NewDataLoader(name string, loader func(ctx context.Context, df *DistributedFilter) (DataLoaderResults, error)) DataLoader {
	return &dataLoaderImpl{
		name:   name,
		loader: loader,
	}
}

type DataLoader interface {
	Name() string
	Load(ctx context.Context, df *DistributedFilter) (DataLoaderResults, error)
}

var RedisStateCheck = NewDataLoader(
	"RedisStateCheck",
	func(ctx context.Context, df *DistributedFilter) (DataLoaderResults, error) {
		df.hooks.Before(RedisFiltersStatesCheck)
		redisBloom := df.redisBloom
		var errorsBatch *multierror.Error
		results := DefaultResults()
		results.Name = "RedisStateCheck"
		results.NeedRunNextLoader = false
		var initializedBucketsCount int
		for bucketID := 0; bucketID < redisBloom.filterParams.BucketsCount; bucketID++ {
			df.hooks.Before(RedisFiltersStateForBucket, bucketID)
			inited, checkErr := redisBloom.checkRedisFilterState(ctx, bucketID)
			if checkErr != nil {
				checkErr = errors.Wrapf(checkErr, "bloom filter init state check error for bucket %d", bucketID)
				df.hooks.AfterFail(RedisFiltersStateForBucket, checkErr, bucketID)
				errorsBatch = multierror.Append(errorsBatch, checkErr)
				continue
			}
			if !inited {
				results.NeedRunNextLoader = true
			} else {
				initializedBucketsCount++
			}
			df.hooks.AfterSuccess(RedisFiltersStateForBucket, bucketID, inited)
		}
		if df.initializedBuckets.len() != df.FilterParams.BucketsCount {
			results.NeedRunNextLoader = true
		}
		df.hooks.After(RedisFiltersStatesCheck, errorsBatch.ErrorOrNil(), initializedBucketsCount, results)
		return results, errorsBatch.ErrorOrNil()
	},
)

var BulkLoaderFromRedis = NewDataLoader(
	"BulkLoaderFromRedis",
	func(ctx context.Context, df *DistributedFilter) (DataLoaderResults, error) {
		df.hooks.Before(BulkLoadingFromRedis)
		redisBloom := df.redisBloom
		var errorsBatch *multierror.Error
		results := DefaultResults()
		results.Name = "BulkLoaderFromRedis"
		results.NeedRunNextLoader = false

		for bucketID := 0; bucketID < redisBloom.filterParams.BucketsCount; bucketID++ {
			df.hooks.Before(BulkLoadingFromRedisForBucket, bucketID)
			var redisFilterBuf bytes.Buffer
			writer := bufio.NewWriter(&redisFilterBuf)
			if _, redisBloomWriteErr := redisBloom.WriteTo(ctx, bucketID, writer); redisBloomWriteErr != nil {
				redisBloomWriteErr = errors.Wrapf(redisBloomWriteErr, "bloom filter load from Redis failed for bucket %d", bucketID)
				df.hooks.AfterFail(BulkLoadingFromRedisForBucket, redisBloomWriteErr, bucketID)
				errorsBatch = multierror.Append(errorsBatch, redisBloomWriteErr)
				continue
			}

			if flushErr := writer.Flush(); flushErr != nil {
				flushErr = errors.Wrapf(flushErr, "bloom filter flush failed for bucket %d", bucketID)
				df.hooks.AfterFail(BulkLoadingFromRedisForBucket, flushErr, bucketID)
				errorsBatch = multierror.Append(errorsBatch, flushErr)
				continue
			}
			results.SourcesPerBucket[bucketID] = redisFilterBuf.Bytes()
			inited, checkErr := redisBloom.checkRedisFilterState(ctx, bucketID)
			if checkErr != nil {
				checkErr = errors.Wrapf(checkErr, "bloom filter init state check error for bucket %d", bucketID)
				df.hooks.AfterFail(BulkLoadingFromRedisForBucket, checkErr, bucketID)
				errorsBatch = multierror.Append(errorsBatch, checkErr)
				continue
			}
			if !inited {
				results.NeedRunNextLoader = true
			} else {
				results.FinalizeFilter = true
			}
			df.hooks.AfterSuccess(BulkLoadingFromRedisForBucket, results, inited)
		}
		df.hooks.After(BulkLoadingFromRedis, errorsBatch.ErrorOrNil(), results)
		return results, errorsBatch.ErrorOrNil()
	},
)

func NewRawDataInMemoryLoader(name string, dataChannelCreator func() <-chan interface{}, initialResults DataLoaderResults) DataLoader {
	return NewDataLoader(
		name,
		func(ctx context.Context, df *DistributedFilter) (DataLoaderResults, error) {
			dataCh := dataChannelCreator()
			for data := range dataCh {
				switch d := data.(type) {
				case string:
					df.inMemory.AddString(d)
				case []byte:
					df.inMemory.Add(d)
				case uint16:
					df.inMemory.AddUint16(d)
				case uint32:
					df.inMemory.AddUint32(d)
				case uint64:
					df.inMemory.AddUint64(d)
				default:
					return initialResults, errors.Wrapf(UnsupportedDataTypeErr, "unsupported data type in channel: %T", d)
				}
			}
			return initialResults, nil
		},
	)
}
