package redisclients

import (
	"context"
	"time"
)

type RedisClient interface {
	// Get returns a byte slice stored under the provided key
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, data []byte, ttl time.Duration) error
	Del(ctx context.Context, key string) error
	GetRange(ctx context.Context, key string, start, end int64) ([]byte, error)
	// CheckBits returns true if all bits at the specified offsets are set to 1
	CheckBits(ctx context.Context, key string, offsets ...uint64) (bool, error)
	BitOpOr(ctx context.Context, dst, source string) error
	Listen(ctx context.Context, channel string) (<-chan string, error)
	Pipeliner(ctx context.Context) Pipeliner
}

type Pipeliner interface {
	Set(key string, data []byte, ttl time.Duration) Pipeliner
	BitOpOr(dst, source string) Pipeliner
	Del(key string) Pipeliner
	BitField(key string, args ...interface{}) Pipeliner
	// SetBits sets bits at the specified offsets to 1
	SetBits(key string, offsets ...uint64) Pipeliner
	Publish(channel string, data []byte) Pipeliner
	Exec() error
}
