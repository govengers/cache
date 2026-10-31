# Redis store

`redisadapter.Store[K, V]`: a `cache.Backend` in Redis, with change events over a Pub/Sub channel. Usable on its own as a typed key-value store or as the backend of a `cache.Synchronized`.

## Usage

Tested ([redisadapter/example_test.go](../../redisadapter/example_test.go), `ExampleNew`):

```go
	store, err := redisadapter.New[string, string](&redisadapter.Options[string]{
		Client:       client,
		KeyCodec:     cache.StringKeyCodec{},
		KeyPrefix:    "greetings",
		TTL:          time.Hour,
		EventChannel: "greetings:events",
	})
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	defer func() {
		if err := store.Close(ctx); err != nil {
			panic(err)
		}
	}()

	changes := make(chan cache.Event[string, string], 1)
	unregister := store.OnEvent(func(event cache.Event[string, string]) {
		if event.Type == cache.EventSet {
			changes <- event
		}
	})
	defer unregister()
	if err := store.AwaitSubscription(ctx); err != nil {
		panic(err)
	}
```

## Options

- `Client` (required): `*redis.Client` from `redis.NewClient` or `redis.NewFailoverClient`. You create, instrument and close it (after `Store.Close`).
- `KeyCodec` (required).
- `KeyPrefix` (required): keys are stored as `<KeyPrefix>:<encoded key>`. `Load` and `RemovePrefix` see only this namespace; give every store sharing a database its own prefix.
- `TTL`: per write, rounded up to whole milliseconds. Zero: forever.
- `EventChannel`: Pub/Sub channel for change events; `New` then starts one goroutine that `Close` stops. Empty: no events (fine for a store nobody synchronizes on).
- `HeartbeatInterval`: subscription health check (one PUBLISH and one INFO on the primary). Default 5s, at most an hour.
- `ScanCount`: `COUNT` hint for `SCAN` in `Load` and `RemovePrefix`.
- `Logger`: dropped subscriptions, undecodable events and entries, panics in handlers.

## Data and events

- Values are MessagePack-encoded (`vmihailenco/msgpack/v5`): exported struct fields only; renaming a field makes old entries decode to zero values. `Load` skips (and logs) entries it cannot decode.
- `Set`, `Remove` and each `RemovePrefix` page run as Lua scripts that change the keys and publish the event together.
- `OnEvent` handlers receive every change on the channel, this store's own included, in Redis's order, one at a time on the receiving goroutine. They must not block. A panicking handler is logged and skipped.
- Handlers receive `cache.EventReset` each time the subscription is (re)established, the first time included, and for undecodable events.
- `AwaitSubscription(ctx)` waits for the first subscription only; it returns `ErrNoEventChannel` without an `EventChannel`.
- The event wire format changed from v0: v0 and v1 stores must not share an event channel.

## Timeouts

go-redis ends socket reads and writes at the context's deadline only with `ContextTimeoutEnabled: true`; otherwise its `ReadTimeout` and `WriteTimeout` (default 3s) do. Set it if `AsyncTimeout` or the contexts you pass should bound Redis calls. The same applies to `Store.Close(ctx)`.

## Redis Sentinel

Pass a Sentinel-aware client:

```go
client := redis.NewFailoverClient(&redis.FailoverOptions{
    MasterName:    "cache",
    SentinelAddrs: []string{"sentinel-0:26379", "sentinel-1:26379", "sentinel-2:26379"},
})
```

After a failover go-redis moves its normal connections to the new primary but leaves Pub/Sub connections on the demoted server. The heartbeat catches it: each interval it publishes a numbered message to a channel of its own and reads the primary's replication ID. A changed replication ID, a missing heartbeat, or none for three to four intervals renews the subscription, which sends `EventReset`. A server that does not report a replication ID (ACL without `INFO`, a proxy) is logged once and leaves that check out. The end-to-end suite ([test/e2e/sentinel_test.go](../../test/e2e/sentinel_test.go)) runs a real failover.

Redis Cluster is not supported: `Load` and `RemovePrefix` scan one node.

## Tracing and metrics

Instrument your client: `redisotel.InstrumentTracing(client)`, `redisotel.InstrumentMetrics(client)`. The store adds nothing of its own.

Source: [redisadapter/store.go](../../redisadapter/store.go), [events.go](../../redisadapter/events.go), [load.go](../../redisadapter/load.go), [internal/subscription](../../internal/subscription/subscription.go).
