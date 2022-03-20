package rtstream

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

// A consumer reads values of type Message[T] from streams
type Consumer[T any] interface {
	Chan() <-chan Message[T]
}

// Stream keys and start ids
type Streams = map[string]string

// Config for a BaseConsumer
type BaseConsumerConfig struct {
	Streams Streams
	Block   time.Duration
	Count   int64
	BufSize int
}

// BaseConsumer operates directly on streams. Calls XREAD
type BaseConsumer[T any] struct {
	Consumer[T] // hint interface implementation
	ctx         context.Context
	rdb         redis.Cmdable
	ch          chan Message[T]
	count       int64
	block       time.Duration
	ids         Streams
	stbuf       []string // buffer for skipping allocations on read
}

// Message receiving channel for this consumer
func (s *BaseConsumer[T]) Chan() <-chan Message[T] {
	return s.ch
}

// Return ids of last read messages from streams
func (sc *BaseConsumer[T]) LastIds() Streams {
	return sc.ids
}

// Create and start new consumer with detailed config
func NewBaseConsumerWithConfig[T any](ctx context.Context, rdb redis.Cmdable, config BaseConsumerConfig) *BaseConsumer[T] {
	cs := &BaseConsumer[T]{
		ctx:   ctx,
		rdb:   rdb,
		ids:   config.Streams,
		block: config.Block,
		count: config.Count,
		ch:    make(chan Message[T], config.BufSize),
		stbuf: make([]string, 2*len(config.Streams)),
	}
	go cs.run()
	return cs
}

// Create new consumer on one or multiple streams
func NewBaseConsumer[T any](ctx context.Context, rdb redis.Cmdable, streams ...string) *BaseConsumer[T] {
	return NewBaseConsumerWithConfig[T](ctx, rdb, BaseConsumerConfig{
		Streams: parseStreams(streams...),
	})
}

// Call XREAD to read the next portion
func (sc *BaseConsumer[T]) read() ([]redis.XStream, error) {
	idx, offset := 0, len(sc.ids)
	for k, v := range sc.ids {
		sc.stbuf[idx] = k
		sc.stbuf[idx+offset] = v
		idx += 1
	}
	res := sc.rdb.XRead(sc.ctx, &redis.XReadArgs{
		Streams: sc.stbuf,
		Block:   sc.block,
		Count:   sc.count,
	})
	return res.Result()
}

// Map incoming messages
func (sc BaseConsumer[T]) handleIncoming(res []redis.XStream) {
	for _, stream := range res {
		lastId := ""
		for _, msg := range stream.Messages {
			sc.ch <- toMessage[T](msg)
			lastId = msg.ID
		}
		sc.ids[stream.Stream] = lastId
	}
}

// Worker goroutine for receiving messages
func (sc BaseConsumer[T]) run() {
	defer close(sc.ch)
	for {
		res, err := sc.read()
		if err != nil {
			// TODO: implement background erorr mechanism
			// maybe alike to rmq
			panic(err)
		}

		sc.handleIncoming(res)

		select {
		case <-sc.ctx.Done():
			return
		default:
			continue
		}
	}
}
