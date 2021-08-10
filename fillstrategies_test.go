package bloom_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/suite"
	"github.com/vkuptcov/bloom"
	"github.com/vkuptcov/bloom/redisclients"
	"syreclabs.com/go/faker"
)

var filterParams = bloom.FilterParams{
	BucketsCount:   10,
	TotalElements:  500,
	FalsePositives: 0.001,
}

type FillStrategiesSuite struct {
	client *redis.Client
	suite.Suite
}

func (st *FillStrategiesSuite) SetupSuite() {
	st.client = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
}

func (st *FillStrategiesSuite) TestCustomFillStrategy() {
	distributedFilter := bloom.NewDistributedFilter(
		redisclients.NewGoRedisClient(st.client),
		"test-bloom-"+faker.RandomString(5),
		filterParams,
		&testFillStrategy{},
	)
	st.Require().NoError(
		distributedFilter.Init(context.Background()),
		"No error expected on filter initialization",
	)
	for i := uint16(0); i < 50; i++ {
		st.Require().True(distributedFilter.TestUint16(i), "data expected in the filter")
	}
}

func TestDistributedFilterSuite(t *testing.T) {
	suite.Run(t, &FillStrategiesSuite{})
}

type testFillStrategy struct{}

func (s *testFillStrategy) DumpStateInRedis() bool {
	return true
}

func (s *testFillStrategy) Sources(ctx context.Context) (map[uint64][]byte, error) {
	f := bloom.NewInMemory(filterParams)
	for i := uint16(0); i < 50; i++ {
		f.AddUint16(i)
	}
	sources := map[uint64][]byte{}
	for bucketID := uint64(0); bucketID < uint64(filterParams.BucketsCount); bucketID++ {
		var buf bytes.Buffer
		_, err := f.WriteTo(bucketID, &buf)
		if err != nil {
			return nil, err
		}
		sources[bucketID] = buf.Bytes()
	}
	return sources, nil
}
