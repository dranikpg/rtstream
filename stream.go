package rtstream

import (
	"context"

	"github.com/go-redis/redis/v8"
)

// Message represent a stream message bound to type T
type Message[T any] struct {
	ID   string
	Data T
}

// A redis stream
type Stream[T any] struct {
	Client redis.Cmdable
	Key    string
}

// Add a message to the stream. Calls XADD
func (s Stream[T]) Add(ctx context.Context, data T, idarg ...string) (string, error) {
	id := ""
	if len(idarg) > 0 {
		id = idarg[0]
	}
	return s.Client.XAdd(ctx, &redis.XAddArgs{
		Stream: s.Key,
		Values: structToMap(data),
		ID:     id,
	}).Result()
}

// Read a portion of the stream. Calls XREAD
// TODO: support pipelining
func (s Stream[T]) Read(ctx context.Context, from, to string, count ...int64) ([]Message[T], error) {
	var redisSlice []redis.XMessage
	var err error
	if len(count) == 0 {
		redisSlice, err = s.Client.XRange(ctx, s.Key, from, to).Result()
	} else {
		redisSlice, err = s.Client.XRangeN(ctx, s.Key, from, to, count[0]).Result()
	}
	if err != nil {
		return nil, err
	}
	msgs := make([]Message[T], len(redisSlice))
	for i, msg := range redisSlice {
		msgs[i] = toMessage[T](msg)
	}
	return msgs, nil
}

// Get stream length. Calls XLEN
func (s Stream[T]) Len(ctx context.Context) (int64, error) {
	return s.Client.XLen(ctx, s.Key).Result()
}
