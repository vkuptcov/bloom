package bloom

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	requireLib "github.com/stretchr/testify/require"
	"syreclabs.com/go/faker"
)

func TestRedisFilter(t *testing.T) {
	require := requireLib.New(t)
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	filter := NewRedisBloom(client, "test", 10, 500, 0.001)

	require.NoError(filter.Add(context.Background(), []byte("abc")), "adding data failed")

	isSet, err := filter.Test(context.Background(), []byte("abc"))
	require.NoError(err, "check data failed")
	require.True(isSet)

	isSet, err = filter.Test(context.Background(), []byte("bca"))
	require.NoError(err, "check data failed")
	require.False(isSet)
}

func TestBloomFiltersEquality(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	const bucketsCount = 50
	const totalElements = 50000
	const falsePositives = 0.001

	redisFilter := NewRedisBloom(client, "test-bloom-"+faker.RandomString(5), bucketsCount, totalElements, falsePositives)
	dbloom := NewDBloom(bucketsCount, totalElements, falsePositives)

	ctx := context.Background()
	start := time.Now().Unix()

	t.Run("insert + get the same", func(t *testing.T) {
		require := requireLib.New(t)
		for i := 0; i < totalElements/2; i++ {
			data := strconv.FormatInt(start, 10) + "_" + strconv.Itoa(i)

			require.NoError(
				redisFilter.Add(ctx, []byte(data)),
				"No error expected on adding data in Redis",
			)
			dbloom.AddString(data)

			redisCheckRes, checkErr := redisFilter.Test(ctx, []byte(data))
			require.NoError(checkErr, "data check in Redis failed")

			imMemoryCheck := dbloom.Test([]byte(data))
			require.Truef(redisCheckRes, "value %q expected in Redis", data)
			require.Truef(imMemoryCheck, "value %q expected in memory", data)
		}
	})

	t.Run("get random", func(t *testing.T) {
		require := requireLib.New(t)
		actualFalsePositives := 0
		const nonExistsChecks = 10000
		for i := 0; i < nonExistsChecks; i++ {
			data := faker.RandomString(7)
			redisCheckRes, checkErr := redisFilter.Test(ctx, []byte(data))
			require.NoError(checkErr, "data check in Redis failed")

			imMemoryCheck := dbloom.Test([]byte(data))
			require.Equal(redisCheckRes, imMemoryCheck, "both filters should respond with the same data")
			if imMemoryCheck {
				actualFalsePositives++
			}
		}
		actualFalsePositivesPercentage := float64(actualFalsePositives) / float64(nonExistsChecks)
		require.InDelta(falsePositives, actualFalsePositivesPercentage, falsePositives*10, "unexpected false positives")
		t.Log(
			"False positives count ",
			actualFalsePositives,
			" out of ",
			nonExistsChecks,
			" checks. Rate: ",
			actualFalsePositivesPercentage,
			". Expected: ",
			falsePositives,
		)
	})
}
