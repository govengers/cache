# govengers/cache

Typed caches and leader election for Go services. Module `github.com/govengers/cache`, Go 1.26, Apache 2.0.

## Model

`Local` is an in-memory LRU cache with an optional TTL. `Synchronized` puts a `Local` in front of a shared `Backend` (the Redis `Store` of `redisadapter`): reads are served locally and fall back to the backend, writes go to both, and every process applies the backend's change events to its own local copy, so a change one process makes reaches the others. `leader.Election` picks one leader among processes sharing a Redis, with a lease and a fencing token per leadership.

Constructors are `New...(options *Options) (*X, error)` and validate at startup. Every type is safe for concurrent use. Values are stored as given, not copied. The library never logs on its own: components report what they cannot return to the optional `Logger *slog.Logger` in their options. You own the `*redis.Client`: create it, instrument it, close it after everything that uses it.

## Packages

| Import `github.com/govengers/cache/...` | Use it for | Page |
|---|---|---|
| (root) `cache` | `Local`, `Synchronized`, `Backend`, `Event`, `KeyCodec` (`StringKeyCodec`, `IntKeyCodec`), `ErrNotFound`, `ErrClosed` | [Local-Cache](Local-Cache.md), [Synchronized-Cache](Synchronized-Cache.md) |
| `redisadapter` | `Store`, the Redis `Backend`, with change events over Pub/Sub | [Redis-Store](Redis-Store.md) |
| `leader` | `Election`: one leader, lease, fencing tokens | [Leader-Election](Leader-Election.md) |

The root package depends on the standard library only; `redisadapter` adds go-redis and msgpack, `leader` go-redis.

## Which type for which need

- One process, values cheap to recompute or private to the process: `Local`. Tested: `ExampleLocal` in [example_test.go](../../example_test.go).
- Several replicas that must see each other's writes, reads mostly local: `Synchronized` on a `redisadapter.Store` with an `EventChannel`. Example: [examples/synchronized](../../examples/synchronized/main.go).
- Redis as a plain typed key-value store, no local copy: `redisadapter.Store` on its own (`EventChannel` empty if nobody listens). Tested: `ExampleNew` in [redisadapter/example_test.go](../../redisadapter/example_test.go).
- Exactly one replica runs a job (scheduler, consumer, migration): `leader.Election`; guard its writes with the fencing token. Example: [examples/leader](../../examples/leader/main.go).
- Shared store other than Redis: implement `cache.Backend`. Example: [examples/custombackend](../../examples/custombackend/main.go), page [Custom-Backend](Custom-Backend.md).
- Redis Sentinel: pass `redis.NewFailoverClient`, nothing else changes. [Redis-Store](Redis-Store.md#redis-sentinel)
- Redis Cluster: not supported.

## Wiring order (Synchronized on Redis)

1. `redis.NewClient` (or `NewFailoverClient`); set `ContextTimeoutEnabled: true` if contexts should bound Redis calls. Close it last.
2. `redisadapter.New` with `KeyPrefix` and `EventChannel`; `defer store.Close(...)`.
3. `cache.NewSynchronized` with `Backend: store`; `defer products.Close(...)` (runs before the store's Close).
4. Optional preload: `store.AwaitSubscription(ctx)`, then `products.Preload(ctx)`.

## Pages

- [Local-Cache](Local-Cache.md): `Local`, TTL and size, prefixes, key codecs.
- [Synchronized-Cache](Synchronized-Cache.md): options, consistency guarantees, async writes, preload, closing.
- [Redis-Store](Redis-Store.md): options, key layout, events, timeouts, Sentinel, tracing.
- [Leader-Election](Leader-Election.md): options, timing, fencing tokens, Sentinel caveat.
- [Custom-Backend](Custom-Backend.md): the `Backend` contract.
- [Migration](Migration.md): from `github.com/mxcd/go-cache` v0.
- [Gotchas](Gotchas.md): the rules that are easy to get wrong.

Reference: [README](../../README.md) (overview), [pkg.go.dev](https://pkg.go.dev/github.com/govengers/cache) (API).
