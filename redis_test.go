package bloom

import (
	"bufio"
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/go-redis/redis/v8"
	requireLib "github.com/stretchr/testify/require"
	"syreclabs.com/go/faker"
)

func TestRedisFilter(t *testing.T) {
	require := requireLib.New(t)
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	filter := NewRedisBloom(client, "test-bloom-"+faker.RandomString(5), 10, 500, 0.001)

	require.NoError(filter.Init(context.Background()), "no error expected on filters initialization")

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
	const bucketsCount = 1
	const totalElements = 100
	const falsePositives = 0.001

	redisFilter := NewRedisBloom(client, "test-bloom-"+faker.RandomString(5), bucketsCount, totalElements, falsePositives)
	inmemory := NewInMemory(bucketsCount, totalElements, falsePositives)

	ctx := context.Background()

	requireLib.NoError(t, redisFilter.Init(ctx))

	t.Run("insert + get the same", func(t *testing.T) {
		require := requireLib.New(t)
		for i := 0; i < int(redisFilter.totalElements); i++ {
			data := []byte(strconv.Itoa(i))

			require.NoError(
				redisFilter.Add(ctx, data),
				"No error expected on adding data in Redis",
			)
			inmemory.Add(data)

			redisCheckRes, checkErr := redisFilter.Test(ctx, data)
			require.NoError(checkErr, "data check in Redis failed")

			imMemoryCheck := inmemory.Test(data)
			require.Truef(redisCheckRes, "value %q expected in Redis", data)
			require.Truef(imMemoryCheck, "value %q expected in memory", data)
		}
	})

	checkBloomFiltersEquality(t, redisFilter, inmemory)

	t.Run("restore bloom filter", func(t *testing.T) {
		require := requireLib.New(t)
		restoredInMemory := NewInMemory(bucketsCount, totalElements, falsePositives)
		for bucketID := uint64(0); bucketID < bucketsCount; bucketID++ {
			var redisFilterBuf bytes.Buffer
			writer := bufio.NewWriter(&redisFilterBuf)
			_, redisBloomWriteErr := redisFilter.WriteTo(context.Background(), bucketID, writer)
			require.NoError(redisBloomWriteErr, "redis filter saving failed")

			require.NoError(writer.Flush())

			require.NoError(
				restoredInMemory.Restore(bucketID, bytes.NewReader(redisFilterBuf.Bytes())),
				"no error expected on bucket restore",
			)
		}
		checkBloomFiltersEquality(t, redisFilter, restoredInMemory)
	})
}

func checkBloomFiltersEquality(t *testing.T, redisFilter *RedisBloom, inmemory *inMemoryBlooms) {
	t.Helper()
	ctx := context.Background()
	falsePositives := redisFilter.falsePositives
	t.Run("get the existing data", func(t *testing.T) {
		require := requireLib.New(t)
		for i := 0; i < int(redisFilter.totalElements); i++ {
			data := []byte(strconv.Itoa(i))

			redisCheckRes, checkErr := redisFilter.Test(ctx, data)
			require.NoError(checkErr, "data check in Redis failed")

			imMemoryCheck := inmemory.Test(data)
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

			imMemoryCheck := inmemory.Test([]byte(data))
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
