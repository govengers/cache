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

Requires Go 1.26. Every type is safe for concurrent use. Values are stored as given, not copied: do not change a value after passing it to `Set` or receiving it from `Get`. The library never logs on its own: components that hit failures they cannot return (a dropped subscription, a failed background write) report them to the optional `Logger` in their options.

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
sessions.Clear()
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

store, err := redisadapter.New[string, Product](&redisadapter.Options[string]{
    Client:       client,
    KeyCodec:     cache.StringKeyCodec{},
    KeyPrefix:    "products",        // required: keys are stored as products:<key>
    TTL:          10 * time.Minute,  // zero: forever
    EventChannel: "products:events", // empty: no change events between processes
    Logger:       logger,            // optional
})
if err != nil {
    return err
}
defer store.Close(context.WithoutCancel(ctx)) // stops receiving events

products, err := cache.NewSynchronized(&cache.SynchronizedOptions[string, Product]{
    Backend:   store, // its KeyCodec is used locally too
    LocalTTL:  time.Minute,
    LocalSize: 1000,
})
if err != nil {
    return err
}
defer products.Close(context.WithoutCancel(ctx)) // ends the cache, drains background writes

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

- **Events follow Redis's order.** `Set` and `Remove` change the key and publish the event in one Lua script, so every subscriber applies changes in the order Redis applied them, and no change is applied without its event.
- **Events arrive at most once.** A change made while a store has no subscription (before the first one is up, or while it reconnects) is never delivered. Each time the subscription is established, the store sends `EventReset`, and `Synchronized` drops its whole local cache, so nothing read before survives. An event that cannot be decoded is turned into `EventReset` too. A subscription that silently stops receiving (a dead connection, a Sentinel failover) is detected by its heartbeat and renewed the same way.
- **Redis expiry sends no event.** A local copy can outlive the shared entry by up to `LocalTTL`, counted from when it was stored locally.
- **A read never undoes a change.** A value read from the backend, or written by a synchronous `Set`, is only kept locally if that key did not change (by an event, a write or a prefix removal covering it) while the call was in flight. Changes to other keys do not interfere.
- **Async writes trade safety for latency.** With `AsyncWrites`, `Set`, `Remove` and `RemovePrefix` change the local cache and return once the backend write is queued; one background goroutine writes to the backend in the order the local cache was changed, each write bounded by `AsyncTimeout` (default 10s). A failure is only logged (a failed `Set` also drops the local copy). At most `AsyncQueueSize` writes (default 1024) wait; beyond that a write blocks until there is room or its context ends.
- **`RemovePrefix` scans.** In Redis it walks the namespace with `SCAN` and removes each page in a script that also publishes the event, so no removed key goes unannounced; keys written while it runs may survive.
- **`Close` ends the cache.** It stops applying events and drains the background writes; every later call returns `ErrClosed`.

### Timeouts

go-redis ends socket reads and writes at the context's deadline only when the client sets `ContextTimeoutEnabled: true`; otherwise its `ReadTimeout` and `WriteTimeout` (default 3s) do. Set it if `AsyncTimeout` or the contexts you pass should bound Redis calls. The leader election does not depend on it: it stops waiting for Redis at its lease deadline either way.

### Redis Sentinel

Pass a Sentinel-aware client; it is a `*redis.Client` like any other, so `Store` and `Election` take it as they are:

```go
client := redis.NewFailoverClient(&redis.FailoverOptions{
    MasterName:    "cache",
    SentinelAddrs: []string{"sentinel-0:26379", "sentinel-1:26379", "sentinel-2:26379"},
})
```

After a failover go-redis moves its normal connections to the new primary, but not its Pub/Sub connections: those stay subscribed on the demoted server and miss what is published on the new one. Every subscription therefore sends a heartbeat every `HeartbeatInterval` (default 5s) through the normal connections: a numbered message to a channel of its own, and a read of the primary's replication ID. It renews itself when the replication ID changes (another server was promoted, or the primary restarted without its data), when a heartbeat goes missing, or when none arrives for three to four intervals. A server that does not report a replication ID (an ACL without `INFO`, a proxy) is logged once and leaves the heartbeat checks. The store then subscribes on the new primary and sends `EventReset`, so no local copy survives the switch. The end-to-end suite runs a real Sentinel failover.

Redis Cluster is not supported: `Load` and `RemovePrefix` scan one node, and the election's scripts touch two keys.

### Tracing and metrics

The store uses your client, so instrument it the go-redis way, e.g. with `redisotel.InstrumentTracing(client)` and `redisotel.InstrumentMetrics(client)`.

### Your own backend

`Synchronized` works with any implementation of `cache.Backend`: `Get` returns an error matching `cache.ErrNotFound` on a miss, `KeyCodec` returns the codec prefixes match on, and `OnEvent` handlers receive every change, including the process's own, in the backend's order, until they are unregistered. Later capabilities (such as an expiry per entry) will come as optional interfaces a backend may implement, never as new methods of `Backend`.

## Leader election

```go
election, err := leader.New(&leader.Options{
    Client:        client,
    LockKey:       "billing:scheduler",
    LeaseDuration: 10 * time.Second,                 // a crashed leader's lock expires after at most this long
    EventChannel:  "billing:scheduler:resignations", // optional: followers take over at once on Stop
    OnElected: func(ctx context.Context, fencingToken int64) {
        runScheduler(ctx, fencingToken) // must return when ctx is cancelled: the leadership ended
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

The leader renews its lease every `RenewInterval` (default a third of the lease, below half). It steps down when the lock was taken over, or when no renewal succeeded by one `RenewInterval` before the lease may expire, even if Redis hangs instead of failing (the election stops waiting for a Redis reply at that deadline, whatever the client's timeouts). When the context given to `Start` ends, the leader steps down and releases the lock. Followers retry every `RetryInterval` (default half the lease) plus jitter. A new term's `OnElected` never starts before the previous one returned.

A leader cut off from Redis can believe it leads for up to one lease minus one `RenewInterval`, and a paused process (GC, a frozen VM) for longer. Guard writes that must never overlap with the **fencing token** `OnElected` receives: it grows with every new leadership, so storage that rejects tokens smaller than the largest it has seen keeps a stale leader out. (`FencingToken()` returns the token of the current term; work of an older term must keep using the token it was given.)

```sql
UPDATE jobs SET state = $1, fencing_token = $2 WHERE id = $3 AND fencing_token <= $2
```

With Sentinel, replication is asynchronous: a failover right after an acquisition can lose the lock and the epoch counter, so a second leader can be elected with a fencing token that was already issued. Fencing tokens are monotonic only as long as no write is lost to a failover.

## Migrating from github.com/mxcd/go-cache

v1.0.0 is the first release under `github.com/govengers/cache`. The old module stays where it is; nothing changes until you switch the import path.

| v0 (`mxcd/go-cache`) | v1 (`govengers/cache`) |
|----------------------|------------------------|
| `CacheKey[K]`, `&StringCacheKey{}`, `&IntCacheKey{}` | `KeyCodec[K]` with `Encode`/`Decode`, `StringKeyCodec{}`, `IntKeyCodec{}`; `MarshallAll`/`UnmarshalAll` are gone |
| `NewLocalCache(options)` panicked without a key | `NewLocal(options) (*Local, error)`; options field `CacheKey` is now `KeyCodec`; new `Clear()` |
| `LocalCache.Load() ([]CacheEntry, error)` | `Local.Entries() []Entry` with `Value V` instead of `*V` |
| `LocalCache.Contains` saw expired entries | `Contains` ignores expired entries |
| `NewRedisStorageBackend(&RedisStorageBackendOptions{...})` built its own client from `RedisOptions` and instrumented it with OpenTelemetry | `redisadapter.New(&redisadapter.Options{...})` takes your `*redis.Client`; instrument it yourself; `Close` no longer closes the client |
| `KeyPrefix` optional | `KeyPrefix` required |
| `PubSub: true` + `PubSubChannelName` | `EventChannel` (empty disables events) |
| `Get` returned `(*V, error)` with `redis.Nil` on a miss | `Get` returns `(V, error)` with `cache.ErrNotFound` on a miss; Redis errors are no longer wrapped |
| `Ttl` | `TTL`: zero for no expiry, `ErrNotFound` for a missing key |
| `AddCallback`, `CacheEvent{Entry, Type, KeyPrefix}` | `OnEvent`, `Event{Type, Key, Value, Prefix}` plus `EventReset`; the wire format changed, so v0 and v1 stores must not share an event channel |
| `NewSynchronizedCache` with `StorageBackend`, `CacheKey`, `Preload`, `Tracer`, `AsyncTimeout` | `NewSynchronized` with `Backend` (its `KeyCodec()` is used); call `Preload(ctx)`; tracing via your Redis client; `AsyncTimeout` bounds each background write, which runs without the caller's cancellation |
| `SynchronizedCache.Get` returned `(*V, bool)` and hid backend errors | `Get` returns `(V, error)` |
| `RemoteAsync` (one goroutine per write, unordered) | `AsyncWrites` (one ordered, bounded queue: `AsyncQueueSize`), plus `Close(ctx)` to drain it; after `Close` every call returns `ErrClosed` |
| `RedisStorageBackend.Close() error` closed the client | `Store.Close(ctx) error` stops receiving events; the client stays yours |
| `NewLeaderElection(LeaderElectionOptions{...})` | `leader.New(&leader.Options{...})` |
| `PubSubChannel` | `EventChannel` |
| `OnElected func(ctx)` | `OnElected func(ctx, fencingToken int64)` |
| `Stop() error` | `Stop(ctx) error`, now waits for `OnElected` to return and returns the release error |
| `RenewInterval` below `LeaseDuration` | `RenewInterval` below half of `LeaseDuration` |
| errors printed with the `log` package | optional `Logger *slog.Logger` in every options struct that needs one |

Values are still MessagePack-encoded under the same keys, so a v1 store reads the entries a v0 backend wrote with the same `KeyPrefix`.

## Development

```sh
just check   # lint, unit tests with -race and the coverage gate, end-to-end tests against Redis in Docker, govulncheck
```

The code follows the [govengers standards](https://github.com/govengers/standards) (library profile).
