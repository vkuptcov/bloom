package bloom

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/suite"
	"github.com/vkuptcov/bloom/redisclients"
	"syreclabs.com/go/faker"
)

type DistributedFilterSuite struct {
	client            *redis.Client
	distributedFilter *DistributedFilter
	suite.Suite
}

func (st *DistributedFilterSuite) SetupSuite() {
	st.client = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
}

func (st *DistributedFilterSuite) SetupTest() {
	st.distributedFilter = NewDistributedFilter(
		redisclients.NewGoRedisClient(st.client),
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
			redisclients.NewGoRedisClient(st.client),
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

//nolint:gocognit //it's a test function with multiple steps
func (st *DistributedFilterSuite) TestSeveralFiltersSync() {
	cachePrefix := "test-bloom-" + faker.RandomString(5)
	fp := FilterParams{
		BucketsCount:   10,
		TotalElements:  10000,
		FalsePositives: 0.001,
	}
	filters := make([]*DistributedFilter, 10)
	wg := &sync.WaitGroup{}

	for filterNum := 0; filterNum < 10; filterNum++ {
		wg.Add(1)
		go func(shift int) {
			filter := NewDistributedFilter(
				redisclients.NewGoRedisClient(st.client),
				cachePrefix,
				fp,
			)
			if shift%2 == 0 {
				filter.SetHooks(&Hooks{
					hooks: map[Stage]HooksInteraction{
						LoadData: &Hook{
							BeforeFn: func(args ...interface{}) {
								time.Sleep(1 * time.Second)
							},
						},
					},
				})
			}

			filters[shift] = filter
			st.Require().NoErrorf(
				filter.Init(
					context.Background()),
				"filter `%d` init failed",
				shift,
			)
			for i := shift * 1000; i < (shift+1)*1000; i++ {
				data := []byte(strconv.Itoa(i))
				st.Require().NoErrorf(
					filter.Add(context.Background(), data),
					"add data error into filter `%d`",
					shift,
				)
			}
			wg.Done()
		}(filterNum)
	}
	wg.Wait()

	time.Sleep(1 * time.Second)

	st.Run("check existed elements", func() {
		for i := 0; i < 10000; i++ {
			for filterID, filter := range filters {
				data := []byte(strconv.Itoa(i))
				st.Require().Truef(
					filter.Test(data),
					"data check `%d` failed for filter `%d`",
					i,
					filterID,
				)
			}
		}
	})

	st.Run("check random elements", func() {
		for attemptNum := 0; attemptNum < 10000; attemptNum++ {
			data := []byte(faker.RandomString(5))
			exists := 0
			nonExists := 0
			for _, filter := range filters {
				if filter.Test(data) {
					exists++
				} else {
					nonExists++
				}
			}
			st.Require().True(
				(exists > 0 && nonExists == 0) ||
					(exists == 0 && nonExists > 0),
				"All filters should respond with the same data",
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
