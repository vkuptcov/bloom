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

var BulkLoaderFromRedis DataLoader = func(ctx context.Context, df *DistributedFilter) (DataLoaderResults, error) {
	redisBloom := df.redisBloom
	var errorsBatch *multierror.Error
	results := DefaultResults()
	results.NeedRunNextLoader = false

	for bucketID := uint64(0); bucketID < uint64(redisBloom.filterParams.BucketsCount); bucketID++ {
		var redisFilterBuf bytes.Buffer
		writer := bufio.NewWriter(&redisFilterBuf)
		if _, redisBloomWriteErr := redisBloom.WriteTo(ctx, bucketID, writer); redisBloomWriteErr != nil {
			errorsBatch = multierror.Append(
				errorsBatch,
				errors.Wrap(redisBloomWriteErr, "bloom filter load from Redis failed"),
			)
			continue
		}

		if flushErr := writer.Flush(); flushErr != nil {
			errorsBatch = multierror.Append(
				errorsBatch,
				errors.Wrap(flushErr, "bloom filter flush failed"),
			)
			continue
		}
		results.SourcesPerBucket[bucketID] = redisFilterBuf.Bytes()
		inited, checkErr := redisBloom.checkRedisFilterState(ctx, bucketID)
		if checkErr != nil {
			errorsBatch = multierror.Append(
				errorsBatch,
				errors.Wrap(checkErr, "bloom filter init state check error"),
			)
			continue
		}
		if !inited {
			results.NeedRunNextLoader = true
		}
	}
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
