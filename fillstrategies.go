package bloom

import (
	"bufio"
	"bytes"
	"context"

	"github.com/pkg/errors"
)

type FillFilterStrategy interface {
	Fill(ctx context.Context) error
}

type FillFilterFromRedis struct {
	df *DistributedFilter
}

func (s *FillFilterFromRedis) Fill(ctx context.Context) error {
	for bucketID := uint64(0); bucketID < uint64(s.df.inMemory.filterParams.BucketsCount); bucketID++ {
		var redisFilterBuf bytes.Buffer
		writer := bufio.NewWriter(&redisFilterBuf)
		if _, redisBloomWriteErr := s.df.redisBloom.WriteTo(ctx, bucketID, writer); redisBloomWriteErr != nil {
			return errors.Wrap(redisBloomWriteErr, "bloom filter load from Redis failed")
		}

		if flushErr := writer.Flush(); flushErr != nil {
			return errors.Wrap(flushErr, "bloom filter flush failed")
		}

		if restoreErr := s.df.inMemory.AddFrom(bucketID, bytes.NewReader(redisFilterBuf.Bytes())); restoreErr != nil {
			return errors.Wrap(restoreErr, "in-memory filter restore failed")
		}
	}
	return nil
}
