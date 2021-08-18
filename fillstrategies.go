package bloom

import (
	"bufio"
	"bytes"
	"context"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

var UnsupportedDataTypeErr = errors.New("unsupported data type")

type DataLoaderResults struct {
	SourcesPerBucket  map[uint64][]byte
	DumpStateInRedis  bool
	NeedRunNextLoader bool
	FinalizeFilter    bool
}

func DefaultResults() DataLoaderResults {
	return DataLoaderResults{
		SourcesPerBucket: map[uint64][]byte{},
	}
}

type DataLoader func(ctx context.Context, df *DistributedFilter) (DataLoaderResults, error)

var RedisStateCheck DataLoader = func(ctx context.Context, df *DistributedFilter) (DataLoaderResults, error) {
	df.hooks.Before(RedisFiltersStateCheck)
	redisBloom := df.redisBloom
	var errorsBatch *multierror.Error
	results := DefaultResults()
	results.NeedRunNextLoader = false
	for bucketID := uint64(0); bucketID < uint64(redisBloom.filterParams.BucketsCount); bucketID++ {
		df.hooks.Before(RedisParticularBucketStateCheck, bucketID)
		inited, checkErr := redisBloom.checkRedisFilterState(ctx, bucketID)
		if checkErr != nil {
			checkErr = errors.Wrapf(checkErr, "bloom filter init state check error for bucket %d", bucketID)
			df.hooks.AfterFail(RedisParticularBucketStateCheck, checkErr, bucketID)
			errorsBatch = multierror.Append(errorsBatch, checkErr)
			continue
		}
		if !inited {
			results.NeedRunNextLoader = true
		}
		df.hooks.AfterSuccess(RedisParticularBucketStateCheck, bucketID, inited)
	}
	if df.initializedBuckets.len() != int(df.FilterParams.BucketsCount) {
		results.NeedRunNextLoader = true
	}
	df.hooks.After(RedisFiltersStateCheck, errorsBatch.ErrorOrNil(), df.initializedBuckets.len(), results)
	return results, errorsBatch.ErrorOrNil()
}

var BulkLoaderFromRedis DataLoader = func(ctx context.Context, df *DistributedFilter) (DataLoaderResults, error) {
	df.hooks.Before(BulkLoadingFromRedis)
	redisBloom := df.redisBloom
	var errorsBatch *multierror.Error
	results := DefaultResults()
	results.NeedRunNextLoader = false

	for bucketID := uint64(0); bucketID < uint64(redisBloom.filterParams.BucketsCount); bucketID++ {
		df.hooks.Before(BulkLoadingFromRedisForParticularBucket, bucketID)
		var redisFilterBuf bytes.Buffer
		writer := bufio.NewWriter(&redisFilterBuf)
		if _, redisBloomWriteErr := redisBloom.WriteTo(ctx, bucketID, writer); redisBloomWriteErr != nil {
			redisBloomWriteErr = errors.Wrapf(redisBloomWriteErr, "bloom filter load from Redis failed for bucket %d", bucketID)
			df.hooks.AfterFail(BulkLoadingFromRedisForParticularBucket, redisBloomWriteErr, bucketID)
			errorsBatch = multierror.Append(errorsBatch, redisBloomWriteErr)
			continue
		}

		if flushErr := writer.Flush(); flushErr != nil {
			flushErr = errors.Wrapf(flushErr, "bloom filter flush failed for bucket %d", bucketID)
			df.hooks.AfterFail(BulkLoadingFromRedisForParticularBucket, flushErr, bucketID)
			errorsBatch = multierror.Append(errorsBatch, flushErr)
			continue
		}
		results.SourcesPerBucket[bucketID] = redisFilterBuf.Bytes()
		inited, checkErr := redisBloom.checkRedisFilterState(ctx, bucketID)
		if checkErr != nil {
			checkErr = errors.Wrapf(checkErr, "bloom filter init state check error for bucket %d", bucketID)
			df.hooks.AfterFail(BulkLoadingFromRedisForParticularBucket, checkErr, bucketID)
			errorsBatch = multierror.Append(errorsBatch, checkErr)
			continue
		}
		if !inited {
			results.NeedRunNextLoader = true
		} else {
			results.FinalizeFilter = true
		}
		df.hooks.AfterSuccess(BulkLoadingFromRedisForParticularBucket, results, inited)
	}
	df.hooks.After(BulkLoadingFromRedis, errorsBatch.ErrorOrNil(), results)
	return results, errorsBatch.ErrorOrNil()
}

func NewRawDataInMemoryLoader(dataChannelCreator func() <-chan interface{}, initialResults DataLoaderResults) DataLoader {
	return func(ctx context.Context, df *DistributedFilter) (DataLoaderResults, error) {
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
	}
}
