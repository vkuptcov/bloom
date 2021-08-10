package bloom

import (
	"bufio"
	"bytes"
	"context"

	"github.com/pkg/errors"
)

var UnsupportedDataTypeErr = errors.New("unsupported data type")

type BulkDataLoader interface {
	// Sources returns a map of bucketIDs to a bytes slice
	Sources(ctx context.Context, df *DistributedFilter) (map[uint64][]byte, error)
	DumpStateInRedis() bool
	NeedRunNextLoader(ctx context.Context, df *DistributedFilter) (bool, error)
}

type BulkLoaderFromRedis struct{}

func (s *BulkLoaderFromRedis) Sources(ctx context.Context, df *DistributedFilter) (map[uint64][]byte, error) {
	redisBloom := df.redisBloom
	sources := make(map[uint64][]byte, redisBloom.filterParams.BucketsCount)
	for bucketID := uint64(0); bucketID < uint64(redisBloom.filterParams.BucketsCount); bucketID++ {
		var redisFilterBuf bytes.Buffer
		writer := bufio.NewWriter(&redisFilterBuf)
		if _, redisBloomWriteErr := redisBloom.WriteTo(ctx, bucketID, writer); redisBloomWriteErr != nil {
			return nil, errors.Wrap(redisBloomWriteErr, "bloom filter load from Redis failed")
		}

		if flushErr := writer.Flush(); flushErr != nil {
			return nil, errors.Wrap(flushErr, "bloom filter flush failed")
		}
		sources[bucketID] = redisFilterBuf.Bytes()
	}
	return sources, nil
}

func (s *BulkLoaderFromRedis) DumpStateInRedis() bool {
	return false
}

func (s *BulkLoaderFromRedis) NeedRunNextLoader(ctx context.Context, df *DistributedFilter) (bool, error) {
	redisBloom := df.redisBloom
	for bucketID := uint64(0); bucketID < uint64(redisBloom.filterParams.BucketsCount); bucketID++ {
		inited, checkErr := redisBloom.checkRedisFilterState(ctx, bucketID)
		if checkErr != nil {
			return false, checkErr
		}
		if !inited {
			return true, nil
		}
	}
	return false, nil
}

type RawDataLoader struct {
	dataChannelCreator func() <-chan interface{}
}

func NewRawDataInMemoryLoader(dataChannelCreator func() <-chan interface{}) *RawDataLoader {
	return &RawDataLoader{dataChannelCreator: dataChannelCreator}
}

func (s *RawDataLoader) Sources(_ context.Context, df *DistributedFilter) (map[uint64][]byte, error) {
	dataCh := s.dataChannelCreator()
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
			return nil, errors.Wrapf(UnsupportedDataTypeErr, "unsupported data type in channel: %T", d)
		}
	}
	return map[uint64][]byte{}, nil
}

func (s *RawDataLoader) DumpStateInRedis() bool {
	return true
}
