package bloom

import (
	"bufio"
	"bytes"
	"context"

	"github.com/pkg/errors"
)

type FillFilterStrategy interface {
	// Sources returns a map of bucketIDs to a bytes slice
	Sources(ctx context.Context) (map[uint64][]byte, error)
}

type FillFilterFromRedis struct {
	df *DistributedFilter
}

func (s *FillFilterFromRedis) Sources(ctx context.Context) (map[uint64][]byte, error) {
	sources := make(map[uint64][]byte, s.df.inMemory.filterParams.BucketsCount)
	for bucketID := uint64(0); bucketID < uint64(s.df.inMemory.filterParams.BucketsCount); bucketID++ {
		var redisFilterBuf bytes.Buffer
		writer := bufio.NewWriter(&redisFilterBuf)
		if _, redisBloomWriteErr := s.df.redisBloom.WriteTo(ctx, bucketID, writer); redisBloomWriteErr != nil {
			return nil, errors.Wrap(redisBloomWriteErr, "bloom filter load from Redis failed")
		}

		if flushErr := writer.Flush(); flushErr != nil {
			return nil, errors.Wrap(flushErr, "bloom filter flush failed")
		}
		sources[bucketID] = redisFilterBuf.Bytes()
	}
	return sources, nil
}
