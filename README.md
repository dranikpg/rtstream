## WIP Typed Redis streams

Redis added [first class streams](https://redis.io/topics/streams-intro) in version 5, but many popular libraries [still don't support them](https://github.com/adjust/rmq/issues/90). Go recently introduced generics. Wouldn't it be great to have a library for typesafe, simple and performant streams?

rtstream currently supports abstractions for reading/writing streams and listening clients for one or multiple streams.

```go
consumer := NewBaseConsumer[Model](ctx, client, "my-stream $")
for msg := range consumer.Chan() {
    // consume msg
}
```

### Planned

* Support for consumer groups
* Background errors
* Tests with a mocked client