package bloom

import (
	"github.com/bits-and-blooms/bloom/v3"
)

type FilterParams struct {
	BucketsCount   int
	TotalElements  uint64
	FalsePositives float64
}

func (fp FilterParams) EstimatedBucketParameters() (bitsCount, hashFunctionsNumber uint) {
	return bloom.EstimateParameters(
		uint(fp.TotalElements)/uint(fp.BucketsCount),
		fp.FalsePositives,
	)
}

func (fp FilterParams) BucketID(data []byte) int {
	return int(bloom.Locations(data, 1)[0] % uint64(fp.BucketsCount))
}

type TestPresence interface {
	Test(data []byte) bool
	TestString(data string) bool
	TestUint16(i uint16) bool
	TestUint32(i uint32) bool
	TestUint64(i uint64) bool
}
