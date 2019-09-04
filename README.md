# go-extensions-redis

This library provides interfaces and implementations for Redis clients and multiplexers. It also
has a very tight relationship with [go-redis](https://github.com/go-redis/redis) in order to reuse
it's signatures and return types.

Clients interfaces are `Client` and `LockerClient`. The last one guarantees that implementers are
both `Client` and `Locker`.

`Locker` implementers must expose functions to lock access to keys, in order to prevent race
conditions. The implementation provided, `BaseClient`, is for a `LockerClient`.

`Mux` is an interface implemented by `BaseMux`. It's purpose is to use multiple redis instances as
shards where operations over a key consistently happen in the same instance during a time interval
either set through a TTL or manually invalidated by calling `Invalidate(Hash) error` present in
`Mux`.

`BaseMux` mappings are also stored and retrieved through a redis client.

`BaseClient` and `BaseMux` have open-tracing support and provide `WithContext(context.Context)`
methods.

## Contribution

If any changes are made to interfaces and/or new interfaces are added, check `make mocks` command
and run it.

## TODO:

- [ ] Tests
  - [ ] BaseMux implements Mux?
  - [ ] BaseClient implements Client?
  - [ ] Tracing
  - [ ] ErrClient
- [X] Mocks
- [X] Docs
- [ ] ErrClient Pipeliner?
