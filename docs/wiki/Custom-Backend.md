# Custom backend

`cache.Synchronized` works with any `cache.Backend[K, V]`. `redisadapter.Store` is the one shipped; implement the interface for another shared store (Postgres with LISTEN/NOTIFY, NATS key-value, ...).

```go
type Backend[K comparable, V any] interface {
	Get(ctx context.Context, key K) (V, error)
	TTL(ctx context.Context, key K) (time.Duration, error)
	Set(ctx context.Context, key K, value V) error
	Remove(ctx context.Context, key K) error
	RemovePrefix(ctx context.Context, prefix string) error
	Contains(ctx context.Context, key K) (bool, error)
	Load(ctx context.Context) ([]Entry[K, V], error)
	KeyCodec() KeyCodec[K]
	OnEvent(handler func(Event[K, V])) (unregister func())
}
```

A complete in-memory implementation with two caches on it: [examples/custombackend/main.go](../../examples/custombackend/main.go) (tested by its `main_test.go`).

## Contract

- `Get` on a missing key returns an error matching `cache.ErrNotFound` (wrap it, `errors.Is` must hold). `TTL` likewise; zero means no expiry. `Contains` returns false, not an error.
- `RemovePrefix(prefix)` removes every key whose `KeyCodec().Encode(key)` starts with `prefix`; an empty prefix removes everything.
- `Load` returns every entry; used by `Synchronized.Preload`.
- `OnEvent` handlers receive every change, the caller's own included, in the order the store applied them, until unregistered: `EventSet` (Key, Value), `EventRemove` (Key), `EventRemovePrefix` (Prefix). `Synchronized` relies on its own writes coming back as events.
- Whenever events may have been lost (reconnect, undecodable message, first subscription), send `EventReset`: every `Synchronized` cache then drops its local copies. A backend that may miss events and never sends `EventReset` leaves stale local copies forever.
- Handlers must be called one at a time; `Synchronized`'s handler never calls back into the backend and does not block.
- Safe for concurrent use.

The change and its event must be atomic with respect to ordering: if two writers race, every subscriber must see the events in the order the store applied the writes. `redisadapter` gets this from one Lua script per change; the memory example from one mutex around change and delivery.

New capabilities (such as an expiry per entry) will come as optional interfaces a backend may implement, never as new methods of `Backend`.

Source: [backend.go](../../backend.go).
