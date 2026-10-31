# Migration from github.com/mxcd/go-cache

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

Pages: [Local-Cache](Local-Cache.md), [Synchronized-Cache](Synchronized-Cache.md), [Redis-Store](Redis-Store.md), [Leader-Election](Leader-Election.md).
