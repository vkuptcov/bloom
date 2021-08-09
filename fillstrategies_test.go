package bloom_test

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/suite"
	"github.com/vkuptcov/bloom"
	"github.com/vkuptcov/bloom/redisclients"
	"syreclabs.com/go/faker"
)

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
		bloom.FilterParams{
			BucketsCount:   10,
			TotalElements:  500,
			FalsePositives: 0.001,
		},
	)
	distributedFilter.Init(context.Background())
}

func TestDistributedFilterSuite(t *testing.T) {
	suite.Run(t, &FillStrategiesSuite{})
}
