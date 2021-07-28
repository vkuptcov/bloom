package bloom

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	requireLib "github.com/stretchr/testify/require"
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
