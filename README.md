# cache

[![CI](https://github.com/govengers/cache/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/govengers/cache/actions/workflows/ci.yml)
[![unit tests](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/govengers/cache/badges/unit-tests.json)](https://github.com/govengers/cache/actions/workflows/ci.yml)
[![e2e tests](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/govengers/cache/badges/e2e-tests.json)](test/e2e)
[![coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/govengers/cache/badges/coverage.json)](https://github.com/govengers/cache/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/govengers/cache.svg)](https://pkg.go.dev/github.com/govengers/cache)
[![License](https://img.shields.io/github/license/govengers/cache)](LICENSE)

Typed caches for Go: an in-memory LRU cache, a local cache kept in step across processes through Redis, and Redis-based leader election.

| Package | What it gives you | Depends on |
|---------|-------------------|------------|
| `cache` | `Local` (LRU with optional TTL), `Synchronized` (local cache in front of a shared `Backend`), `KeyCodec`, `ErrNotFound` | standard library |
| `cache/redisadapter` | `Store`, the Redis `Backend`, with change events over Pub/Sub | go-redis, msgpack |
| `cache/leader` | `Election`: one leader among processes, with a lease and fencing tokens | go-redis |

```sh
go get github.com/govengers/cache
```

Requires Go 1.26. Every type is safe for concurrent use. The library never logs on its own: components that hit failures they cannot return (a dropped subscription, a failed background write) report them to the optional `Logger` in their options.

## Local cache

```go
sessions, err := cache.NewLocal[string, int](&cache.LocalOptions[string]{
    KeyCodec: cache.StringKeyCodec{}, // required
    TTL:      time.Hour,              // zero: entries never expire
    Size:     10000,                  // zero: unlimited
})
if err != nil {
    return err
}

sessions.Set("session:alice", 42)
userID, found := sessions.Get("session:alice")
sessions.Remove("session:alice")
sessions.RemovePrefix("session:") // every key whose encoding starts with the prefix
```

`Local` starts no goroutines and needs no `Close`. Expired entries are invisible at once and reclaimed by the next write after a TTL has passed.

### Keys

A `KeyCodec` turns keys into the strings that prefixes match on and Redis stores. `StringKeyCodec` and `IntKeyCodec` are included; for other key types implement

```go
type KeyCodec[K comparable] interface {
    Encode(key K) string
    Decode(encoded string) (K, error)
}
```

`Encode` must give distinct keys distinct strings, `Decode` must invert it.

## Synchronized cache

`Synchronized` serves reads from a local cache and falls back to a shared `Backend`. Writes go to both. Every process sharing the backend applies its change events to its own local cache, so a change one process makes reaches the others.

```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"}) // you own it: instrument it, close it last
defer client.Close()

store, err := redisadapter.NewStore[string, Product](&redisadapter.StoreOptions[string]{
    Client:       client,
    KeyCodec:     cache.StringKeyCodec{},
    KeyPrefix:    "products",        // keys are stored as products:<key>
    TTL:          10 * time.Minute,  // zero: forever
    EventChannel: "products:events", // empty: no change events between processes
    Logger:       logger,            // optional
})
if err != nil {
    return err
}
defer store.Close()

products, err := cache.NewSynchronized(&cache.SynchronizedOptions[string, Product]{
    Backend:   store,
    KeyCodec:  cache.StringKeyCodec{}, // must match the store's
    LocalTTL:  time.Minute,
    LocalSize: 1000,
})
if err != nil {
    return err
}
defer products.Close()

// Optional: copy every entry into the local cache, once the store receives events. The store
// resets the local cache whenever its subscription is (re)established, so a preload before that
// would be dropped again.
if err := store.AwaitSubscription(ctx); err != nil {
    return err
}
if err := products.Preload(ctx); err != nil {
    return err
}

err = products.Set(ctx, "desk", Product{Name: "desk", PriceCents: 24900})
desk, err := products.Get(ctx, "desk")
if errors.Is(err, cache.ErrNotFound) {
    // not cached anywhere
}
```

### Consistency

- **Events arrive at most once.** A change made while a store has no subscription (before the first one is up, or while it reconnects) is never delivered. Each time the subscription is established, the store sends `EventReset`, and `Synchronized` drops its whole local cache, so nothing read before survives. A subscription that silently stops receiving (a dead connection, a Sentinel failover) is detected by its heartbeat and renewed the same way.
- **Redis expiry sends no event.** Keep `LocalTTL` at most the store's `TTL`, or a local copy outlives the shared entry.
- **A read never undoes a change.** A value read from the backend is only kept locally if no change was applied while the read was in flight.
- **Async writes trade safety for latency.** With `AsyncWrites`, `Set`, `Remove` and `RemovePrefix` change the local cache and return at once; the backend write runs in the background and a failure is only logged (a failed `Set` also drops the local copy). `Close` waits for pending background writes.
- **`RemovePrefix` scans.** In Redis it walks the namespace with `SCAN` and `UNLINK`s in pages; keys written while it runs may survive.

### Redis Sentinel

Pass a Sentinel-aware client; it is a `*redis.Client` like any other, so `Store` and `Election` take it as they are:

```go
client := redis.NewFailoverClient(&redis.FailoverOptions{
    MasterName:    "cache",
    SentinelAddrs: []string{"sentinel-0:26379", "sentinel-1:26379", "sentinel-2:26379"},
})
```

After a failover go-redis moves its normal connections to the new primary, but not its Pub/Sub connections: those stay subscribed on the demoted server and miss what is published on the new one. Every subscription therefore publishes numbered heartbeats to a channel of its own through the normal connections, and renews itself when one goes missing or none arrives for three `HeartbeatInterval`s (default 5s). The store then subscribes on the new primary and sends `EventReset`, so no local copy survives the switch. The end-to-end suite runs a real Sentinel failover.

Redis Cluster is not supported: `Load` and `RemovePrefix` scan one node, and the election's scripts touch two keys.

### Tracing and metrics

The store uses your client, so instrument it the go-redis way, e.g. with `redisotel.InstrumentTracing(client)` and `redisotel.InstrumentMetrics(client)`.

### Your own backend

`Synchronized` works with any implementation of `cache.Backend`: `Get` returns an error matching `cache.ErrNotFound` on a miss, and `OnEvent` handlers receive every change, including the process's own.

## Leader election

```go
election, err := leader.NewElection(&leader.ElectionOptions{
    Client:        client,
    LockKey:       "billing:scheduler",
    LeaseDuration: 10 * time.Second,                 // a crashed leader is replaced after at most this long
    EventChannel:  "billing:scheduler:resignations", // optional: followers take over at once on Stop
    OnElected: func(ctx context.Context) {
        runScheduler(ctx) // must return when ctx is cancelled: the leadership ended
    },
    OnDemoted: func() {},
})
if err != nil {
    return err
}
if err := election.Start(ctx); err != nil {
    return err
}
defer election.Stop(context.WithoutCancel(ctx)) // releases the lock, waits for OnElected
```

The leader renews its lease every `RenewInterval` (default a third of the lease, at most half). It steps down when the lock was taken over, or when no renewal succeeded by one `RenewInterval` before the lease may expire, even if Redis hangs instead of failing. When the context given to `Start` ends, the leader steps down and releases the lock. Followers retry every `RetryInterval` (default half the lease) plus jitter.

A leader cut off from Redis can believe it leads for up to one lease minus one `RenewInterval`, and a paused process (GC, a frozen VM) for longer. Guard writes that must never overlap with the **fencing token**: `FencingToken()` grows with every new leadership, so storage that rejects tokens smaller than the largest it has seen keeps a stale leader out.

```sql
UPDATE jobs SET state = $1, fencing_token = $2 WHERE id = $3 AND fencing_token <= $2
```

With Sentinel, replication is asynchronous: a failover right after an acquisition can lose the lock and the epoch counter, so a second leader can be elected with a fencing token that was already issued. Fencing tokens are monotonic only as long as no write is lost to a failover.

## Migrating from github.com/mxcd/go-cache

v1.0.0 is the first release under `github.com/govengers/cache`. The old module stays where it is; nothing changes until you switch the import path.

| v0 (`mxcd/go-cache`) | v1 (`govengers/cache`) |
|----------------------|------------------------|
| `CacheKey[K]`, `&StringCacheKey{}`, `&IntCacheKey{}` | `KeyCodec[K]` with `Encode`/`Decode`, `StringKeyCodec{}`, `IntKeyCodec{}`; `MarshallAll`/`UnmarshalAll` are gone |
| `NewLocalCache(options)` panicked without a key | `NewLocal(options) (*Local, error)`; options field `CacheKey` is now `KeyCodec` |
| `LocalCache.Load() ([]CacheEntry, error)` | `Local.Entries() []Entry` with `Value V` instead of `*V` |
| `LocalCache.Contains` saw expired entries | `Contains` ignores expired entries |
| `NewRedisStorageBackend` built its own client from `RedisOptions` and instrumented it with OpenTelemetry | `redisadapter.NewStore` takes your `*redis.Client`; instrument it yourself; `Close` no longer closes the client |
| `PubSub: true` + `PubSubChannelName` | `EventChannel` (empty disables events) |
| `Get` returned `(*V, error)` with `redis.Nil` on a miss | `Get` returns `(V, error)` with `cache.ErrNotFound` on a miss; Redis errors are no longer wrapped |
| `Ttl` | `TTL`: zero for no expiry, `ErrNotFound` for a missing key |
| `AddCallback`, `CacheEvent{Entry, Type, KeyPrefix}` | `OnEvent`, `Event{Type, Key, Value, Prefix}` plus `EventReset`; the wire format changed, so v0 and v1 stores must not share an event channel |
| `NewSynchronizedCache` with `StorageBackend`, `Preload`, `Tracer`, `AsyncTimeout` | `NewSynchronized` with `Backend`; call `Preload(ctx)`; tracing via your Redis client; background writes run without the caller's cancellation, bounded by the client's timeouts |
| `SynchronizedCache.Get` returned `(*V, bool)` and hid backend errors | `Get` returns `(V, error)` |
| `RemoteAsync` | `AsyncWrites`, plus `Close()` to wait for background writes |
| `NewLeaderElection(LeaderElectionOptions{...})` | `leader.NewElection(&leader.ElectionOptions{...})` |
| `PubSubChannel` | `EventChannel` |
| `Stop() error` | `Stop(ctx) error`, now waits for `OnElected` to return and returns the release error |
| `RenewInterval` below `LeaseDuration` | `RenewInterval` below half of `LeaseDuration` |
| errors printed with the `log` package | optional `Logger *slog.Logger` in every options struct that needs one |

Values are still MessagePack-encoded under the same keys, so a v1 store reads the entries a v0 backend wrote with the same `KeyPrefix`.

## Development

```sh
just check   # lint, unit tests with -race and the coverage gate, end-to-end tests against Redis in Docker, govulncheck
```

The code follows the [govengers standards](https://github.com/govengers/standards) (library profile).
