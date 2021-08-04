package bloom

import (
	"github.com/bits-and-blooms/bloom/v3"
)

type FilterParams struct {
	BucketsCount   uint32
	TotalElements  uint64
	FalsePositives float64
}

func (fp FilterParams) EstimatedBucketParameters() (bitsCount, hashFunctionsNumber uint) {
	return bloom.EstimateParameters(
		uint(fp.TotalElements)/uint(fp.BucketsCount),
		fp.FalsePositives,
	)
}

func (fp FilterParams) BucketID(data []byte) uint64 {
	return bloom.Locations(data, 1)[0] % uint64(fp.BucketsCount)
}