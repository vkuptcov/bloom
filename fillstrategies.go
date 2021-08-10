package bloom

import (
	"bufio"
	"bytes"
	"context"

	"github.com/pkg/errors"
)

type BulkDataLoader interface {
	// Sources returns a map of bucketIDs to a bytes slice
	Sources(ctx context.Context) (map[uint64][]byte, error)
	DumpStateInRedis() bool
}

type BulkLoaderFromRedis struct {
	redisBloom *RedisBloom
}

func (s *BulkLoaderFromRedis) Sources(ctx context.Context) (map[uint64][]byte, error) {
	sources := make(map[uint64][]byte, s.redisBloom.filterParams.BucketsCount)
	for bucketID := uint64(0); bucketID < uint64(s.redisBloom.filterParams.BucketsCount); bucketID++ {
		var redisFilterBuf bytes.Buffer
		writer := bufio.NewWriter(&redisFilterBuf)
		if _, redisBloomWriteErr := s.redisBloom.WriteTo(ctx, bucketID, writer); redisBloomWriteErr != nil {
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
