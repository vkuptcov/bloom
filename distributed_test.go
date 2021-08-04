package bloom

import (
	"context"
	"strconv"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/suite"
	"syreclabs.com/go/faker"
)

type DistributedFilterSuite struct {
	client            *redis.Client
	distributedFilter *distributedFilter
	suite.Suite
}

func (st *DistributedFilterSuite) SetupSuite() {
	st.client = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
}

func (st *DistributedFilterSuite) SetupTest() {
	st.distributedFilter = NewDistributedFilter(
		st.client,
		"test-bloom-"+faker.RandomString(5),
		FilterParams{
			BucketsCount:   10,
			TotalElements:  500,
			FalsePositives: 0.001,
		},
	)
}

func (st *DistributedFilterSuite) TestInit() {
	st.init()
}

func (st *DistributedFilterSuite) TestAdd() {
	st.init()
	st.Run("fill filter", func() {
		for i := 0; i < 200; i++ {
			data := []byte(strconv.Itoa(i))
			st.Require().NoError(
				st.distributedFilter.Add(context.Background(), data),
				"No error expected on adding a new data",
			)

			st.Require().True(
				st.distributedFilter.Test(data),
				"data expected in the distributed filter",
			)

		}
	})

	st.Run("restore filter", func() {
		restoredFilter := NewDistributedFilter(
			st.client,
			st.distributedFilter.redisBloom.cachePrefix,
			st.distributedFilter.redisBloom.filterParams,
		)

		st.Require().NoError(
			restoredFilter.Init(context.Background()),
			"No error expected on filter restore",
		)

		for i := 0; i < 200; i++ {
			data := []byte(strconv.Itoa(i))
			st.Require().True(
				restoredFilter.Test(data),
				"data expected in the distributed filter",
			)
		}
	})
}

func (st *DistributedFilterSuite) init() {
	st.T().Helper()
	st.Require().NoError(
		st.distributedFilter.Init(context.Background()),
		"No error expected on filter init",
	)
}

func TestDistributedFilterSuite(t *testing.T) {
	suite.Run(t, &DistributedFilterSuite{})
}
