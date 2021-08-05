package bloom

import (
	"context"

	"github.com/go-redis/redis/v8"
)

type RedisClient interface {
	// Get returns a byte slice stored under the provided key
	Get(ctx context.Context, key string) ([]byte, error)
	// CheckBits returns true if all bits at the specified offsets are set to 1
	CheckBits(ctx context.Context, key string, offsets ...uint64) (bool, error)
	Listen(ctx context.Context, channel string) (<-chan *Message, error)
	Pipeliner(ctx context.Context) Pipeliner
}

type Pipeliner interface {
	// SetBits sets bits at the specified offsets to 1
	SetBits(key string, offsets ...uint64) Pipeliner
	Publish(channel string, data []byte) Pipeliner
	Exec() error
}

type Message struct {
	Channel      string
	Pattern      string
	Payload      string
	PayloadSlice []string
}

type goRedisClient struct {
	client redis.UniversalClient
}

func NewGoRedisClient(client redis.UniversalClient) RedisClient {
	return &goRedisClient{client: client}
}

func (g *goRedisClient) Get(ctx context.Context, key string) ([]byte, error) {
	stringCmd := g.client.Get(ctx, key)
	return stringCmd.Bytes()
}

func (g *goRedisClient) CheckBits(ctx context.Context, key string, offsets ...uint64) (bool, error) {
	bitFieldArgs := make([]interface{}, 0, len(offsets)*3)
	for _, offset := range offsets {
		bitFieldArgs = append(bitFieldArgs, "GET", "u1", offset)
	}
	sliceCmds := g.client.BitField(ctx, key, bitFieldArgs...)
	res, err := sliceCmds.Result()
	if err != nil {
		return false, err
	}
	for _, s := range res {
		if s == 0 {
			return false, nil
		}
	}
	return true, nil
}

func (g *goRedisClient) Listen(ctx context.Context, channel string) (<-chan *Message, error) {
	pubSub := g.client.Subscribe(ctx, channel)
	// Wait for confirmation that subscription is created
	if _, receiveErr := pubSub.Receive(ctx); receiveErr != nil {
		return nil, receiveErr
	}
	redisChannel := pubSub.Channel()
	messages := make(chan *Message, cap(redisChannel))
	go func() {
		for m := range redisChannel {
			messages <- (*Message)(m)
		}
		close(messages)
	}()
	return messages, nil
}

func (g *goRedisClient) Pipeliner(ctx context.Context) Pipeliner {
	return &goRedisPipeliner{
		ctx:       ctx,
		pipeliner: g.client.Pipeline(),
	}
}

type goRedisPipeliner struct {
	ctx       context.Context
	pipeliner redis.Pipeliner
}

func (g *goRedisPipeliner) SetBits(key string, offsets ...uint64) Pipeliner {
	bitFieldArgs := make([]interface{}, 0, len(offsets)*4)
	for _, offset := range offsets {
		bitFieldArgs = append(bitFieldArgs, "SET", "u1", offset, 1)
	}
	g.pipeliner.BitField(g.ctx, key, bitFieldArgs...)
	return g
}

func (g *goRedisPipeliner) Publish(channel string, data []byte) Pipeliner {
	g.pipeliner.Publish(g.ctx, channel, data)
	return g
}

func (g *goRedisPipeliner) Exec() error {
	_, err := g.pipeliner.Exec(g.ctx)
	return err
}

var _ RedisClient = &goRedisClient{}
