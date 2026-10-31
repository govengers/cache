# Synchronized cache

`cache.Synchronized[K, V]`: a `Local` cache in front of a shared `cache.Backend`. Reads are served locally and fall back to the backend, writes go to both, and every process sharing the backend applies its change events to its own local cache.

## Setup

From [examples/synchronized/main.go](../../examples/synchronized/main.go) (tested by its `main_test.go`), one replica:

```go
	store, err := redisadapter.New[string, Product](&redisadapter.Options[string]{
		Client:       client,
		KeyCodec:     cache.StringKeyCodec{},
		KeyPrefix:    "products",
		TTL:          time.Hour,
		EventChannel: "products:events",
	})
	if err != nil {
		return nil, err
	}
	products, err := cache.NewSynchronized(&cache.SynchronizedOptions[string, Product]{
		Backend:   store,
		LocalTTL:  time.Minute, // bounds how long a local copy outlives an entry Redis expired
		LocalSize: 1000,
	})
	if err != nil {
		return nil, errors.Join(err, store.Close(ctx))
	}
	started := &replica{store: store, products: products}
	// Preload only after the first subscription is up: its EventReset clears the local cache.
	if err := store.AwaitSubscription(ctx); err != nil {
		return nil, errors.Join(err, started.close(context.WithoutCancel(ctx)))
	}
	if err := products.Preload(ctx); err != nil {
		return nil, errors.Join(err, started.close(context.WithoutCancel(ctx)))
	}
```

Shut down in reverse: `products.Close`, then `store.Close`, then the Redis client.

## Options

- `Backend` (required): the shared store; its `KeyCodec()` is used for the local cache too.
- `LocalTTL`: how long a local copy lives after it was stored locally. Zero: no expiry. Set it whenever the backend has a TTL: Redis sends no event when a key expires, so a local copy outlives the shared entry by up to `LocalTTL`.
- `LocalSize`: maximum local entries. Zero: unlimited.
- `AsyncWrites`, `AsyncQueueSize` (default 1024), `AsyncTimeout` (default 10s): see [Async writes](#async-writes).
- `Logger`: failed background writes and panics in them. Nil: nothing is logged.

## Methods

All take a `context.Context` and return `ErrClosed` after `Close`.

- `Get(ctx, key) (V, error)`: local, else backend (kept locally); a miss matches `cache.ErrNotFound`. Backend errors are returned, not hidden.
- `Set`, `Remove`, `RemovePrefix(ctx, prefix)`: backend and local cache.
- `Contains(ctx, key)`: local, else backend. `TTL(ctx, key)`: always the backend.
- `Preload(ctx)`: copies every backend entry into the local cache, except keys that changed while it ran.
- `Close(ctx)`: stops applying events, drains async writes; safe to call twice; does not close the backend.

## Consistency

- **Events follow the backend's order.** The Redis store changes the key and publishes the event in one Lua script, so every subscriber applies changes in the order Redis applied them, and no change is applied without its event.
- **Events arrive at most once.** A change made while a store has no subscription (before the first one is up, or while it reconnects) is never delivered. Each time the subscription is established the store sends `EventReset`, and `Synchronized` drops its whole local cache. An undecodable event becomes `EventReset` too. A subscription that silently stops receiving is detected by its heartbeat and renewed the same way.
- **Propagation is asynchronous.** After process A's `Set` returns, process B serves its old local copy until the event arrives (normally milliseconds). Read-your-own-writes holds within one process; across processes it is eventual. Tests across replicas must poll (see `eventually` in the example).
- **Redis expiry sends no event.** A local copy can outlive the shared entry by up to `LocalTTL`.
- **A read never undoes a change.** A value read from the backend, or written by a synchronous `Set`, is kept locally only if that key did not change (event, write, or a prefix removal covering it) while the call was in flight.
- **`RemovePrefix` scans.** In Redis it walks the namespace with `SCAN`; keys written while it runs may survive.

## Async writes

With `AsyncWrites: true`, `Set`, `Remove` and `RemovePrefix` change the local cache and return once the backend write is queued. One background goroutine writes to the backend in the order the local cache was changed, each write bounded by `AsyncTimeout` (it replaces the caller's deadline). A failure is only logged; a failed `Set` also drops the local copy, a failed removal leaves the backend's value in place. When `AsyncQueueSize` writes wait, a further write blocks until there is room or its context ends (then it returns the context error). `Close(ctx)` drains the queue.

Use it when write latency matters more than knowing the write reached Redis. Do not use it for values other processes must see before the caller continues.

## Preload

`Preload` fills the local cache from `Backend.Load`. With `redisadapter`, call `store.AwaitSubscription(ctx)` first: the first subscription's `EventReset` would drop everything preloaded before it. A later reconnect drops it again; entries then come back on demand.

Source: [synchronized.go](../../synchronized.go), [fence.go](../../fence.go) (read fence), [background.go](../../background.go) (async queue).
