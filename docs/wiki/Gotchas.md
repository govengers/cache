# Gotchas

The rules that are easy to get wrong, one line each with the page that explains it.

## Values and keys

- Values are stored as given, not copied: never change a map, slice or pointer value after `Set` or `Get`. [Local-Cache](Local-Cache.md#gotchas)
- Redis values are MessagePack: exported fields only, a renamed field decodes old entries to its zero value. [Redis-Store](Redis-Store.md#data-and-events)
- `KeyCodec.Encode` must be injective and prefix-friendly; `RemovePrefix` matches on the encoded string, not on `K`. [Local-Cache](Local-Cache.md#keys)
- `KeyPrefix` is per store; two stores with the same prefix in one database see and clear each other's keys. [Redis-Store](Redis-Store.md#options)

## Synchronized

- Cross-process propagation is eventual: right after process A's `Set`, process B may still serve its old copy. Tests across replicas poll. [Synchronized-Cache](Synchronized-Cache.md#consistency)
- Set `LocalTTL` when the store has a `TTL`: Redis expiry sends no event. [Synchronized-Cache](Synchronized-Cache.md#options)
- No `EventChannel` on the store means no synchronization: each process keeps stale copies until `LocalTTL` (forever with zero). [Redis-Store](Redis-Store.md#options)
- Call `store.AwaitSubscription(ctx)` before `Preload`, or the first `EventReset` drops the preload. [Synchronized-Cache](Synchronized-Cache.md#preload)
- `AsyncWrites` errors never reach the caller; they go to `Logger`. Pass a `Logger`. [Synchronized-Cache](Synchronized-Cache.md#async-writes)
- `Synchronized.Close` does not close the store, `Store.Close` does not close the client: close cache, store, client in that order. [Synchronized-Cache](Synchronized-Cache.md#setup)
- Every call after `Close` returns `cache.ErrClosed`.
- `OnEvent` handlers run on the store's receiving goroutine: a blocking handler stalls every event of that store. [Redis-Store](Redis-Store.md#data-and-events)

## Redis

- Contexts bound Redis calls only with `ContextTimeoutEnabled: true` on the client. [Redis-Store](Redis-Store.md#timeouts)
- Redis Cluster is not supported; Sentinel is (via `redis.NewFailoverClient`). [Redis-Store](Redis-Store.md#redis-sentinel)
- v0 (`mxcd/go-cache`) and v1 stores must not share an event channel; keys and values are compatible. [Migration](Migration.md)

## Leader election

- `OnElected` must return when its `ctx` ends, or `Stop` never returns. [Leader-Election](Leader-Election.md#options)
- `OnElected` and `OnDemoted` must not call `Stop`.
- `IsLeader()` is not a guarantee: guard overlapping writes with the fencing token `OnElected` received, not with `FencingToken()`. [Leader-Election](Leader-Election.md#fencing-tokens)
- `RenewInterval` must be below half of `LeaseDuration`. [Leader-Election](Leader-Election.md#options)
- Without `EventChannel` a successor takes over only at its next `RetryInterval`. [Leader-Election](Leader-Election.md#options)
- With Sentinel a failover can reissue a fencing token. [Leader-Election](Leader-Election.md#sentinel-caveat)
