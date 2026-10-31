# cache

[![CI](https://github.com/govengers/cache/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/govengers/cache/actions/workflows/ci.yml)
[![unit tests](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/govengers/cache/badges/unit-tests.json)](https://github.com/govengers/cache/actions/workflows/ci.yml)
[![e2e tests](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/govengers/cache/badges/e2e-tests.json)](test/e2e)
[![coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/govengers/cache/badges/coverage.json)](https://github.com/govengers/cache/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/govengers/cache.svg)](https://pkg.go.dev/github.com/govengers/cache)
[![License](https://img.shields.io/github/license/govengers/cache)](LICENSE)

Coding agents: start at [llms.txt](llms.txt) and [docs/wiki/Home.md](docs/wiki/Home.md); runnable programs are in [examples/](examples/).

Typed caches for Go: an in-memory LRU cache, a local cache kept in step across processes through Redis, and Redis-based leader election.

| Package | What it gives you | Depends on |
|---------|-------------------|------------|
| `cache` | `Local` (LRU with optional TTL), `Synchronized` (local cache in front of a shared `Backend`), `KeyCodec`, `ErrNotFound` | standard library |
| `cache/redisadapter` | `Store`, the Redis `Backend`, with change events over Pub/Sub | go-redis, msgpack |
| `cache/leader` | `Election`: one leader among processes, with a lease and fencing tokens | go-redis |

```sh
go get github.com/govengers/cache
```

Requires Go 1.26. Every type is safe for concurrent use. Values are stored as given, not copied. The library never logs on its own; components report what they cannot return to the optional `Logger` in their options. You own the `*redis.Client`. Redis Sentinel is supported, Redis Cluster is not.

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
```

More: [Local-Cache](docs/wiki/Local-Cache.md).

## Synchronized cache

Reads are served locally and fall back to Redis; writes go to both; every process applies the store's change events to its own local copy.

```go
store, err := redisadapter.New[string, Product](&redisadapter.Options[string]{
    Client:       client,            // yours: instrument it, close it last
    KeyCodec:     cache.StringKeyCodec{},
    KeyPrefix:    "products",        // required: keys are stored as products:<key>
    TTL:          10 * time.Minute,
    EventChannel: "products:events", // empty: no change events between processes
})
if err != nil {
    return err
}
defer store.Close(context.WithoutCancel(ctx))

products, err := cache.NewSynchronized(&cache.SynchronizedOptions[string, Product]{
    Backend:  store,
    LocalTTL: time.Minute, // Redis expiry sends no event; this bounds stale local copies
})
if err != nil {
    return err
}
defer products.Close(context.WithoutCancel(ctx))

err = products.Set(ctx, "desk", Product{Name: "desk", PriceCents: 24900})
desk, err := products.Get(ctx, "desk") // errors.Is(err, cache.ErrNotFound) on a miss
```

Propagation between processes is eventual and events arrive at most once (a reconnect resets every local cache). Guarantees, async writes and preload: [Synchronized-Cache](docs/wiki/Synchronized-Cache.md); Redis options, timeouts and Sentinel: [Redis-Store](docs/wiki/Redis-Store.md). Runnable: [examples/synchronized](examples/synchronized/main.go).

## Leader election

```go
election, err := leader.New(&leader.Options{
    Client:        client,
    LockKey:       "billing:scheduler",
    LeaseDuration: 10 * time.Second,
    EventChannel:  "billing:scheduler:resignations", // optional: followers take over at once on Stop
    OnElected: func(ctx context.Context, fencingToken int64) {
        runScheduler(ctx, fencingToken) // must return when ctx is cancelled
    },
})
if err != nil {
    return err
}
if err := election.Start(ctx); err != nil {
    return err
}
defer election.Stop(context.WithoutCancel(ctx))
```

Guard writes that must never overlap with the fencing token. Timing, fencing and the Sentinel caveat: [Leader-Election](docs/wiki/Leader-Election.md). Runnable: [examples/leader](examples/leader/main.go).

## Docs

[Wiki home](docs/wiki/Home.md) · [Custom backend](docs/wiki/Custom-Backend.md) · [Gotchas](docs/wiki/Gotchas.md) · [Migration from mxcd/go-cache](docs/wiki/Migration.md) · [pkg.go.dev](https://pkg.go.dev/github.com/govengers/cache) · changelog in [GitHub Releases](https://github.com/govengers/cache/releases)

## Development

```sh
just check   # lint, unit tests with -race and the coverage gate, end-to-end tests against Redis in Docker, govulncheck
```

The code follows the [govengers standards](https://github.com/govengers/standards) (library profile).
