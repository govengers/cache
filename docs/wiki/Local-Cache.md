# Local cache

`cache.Local[K, V]`: an in-memory LRU cache with an optional TTL. No goroutines, no `Close`.

## Usage

Tested ([example_test.go](../../example_test.go), `ExampleLocal`):

```go
	sessions, err := cache.NewLocal[string, int](&cache.LocalOptions[string]{
		KeyCodec: cache.StringKeyCodec{},
		TTL:      time.Hour,
		Size:     10000,
	})
	if err != nil {
		panic(err)
	}

	sessions.Set("session:alice", 42)
	sessions.Set("session:bob", 7)
	sessions.RemovePrefix("session:b")

	userID, found := sessions.Get("session:alice")
```

## Options

- `KeyCodec` (required): encodes keys for `RemovePrefix`.
- `TTL`: how long an entry lives after its last `Set`. Zero: never expires. Reads do not extend it.
- `Size`: maximum entries; a write beyond it drops the least recently used one. Zero: unlimited. `Get` and `Set` make an entry the most recently used; `Contains` and `Entries` do not.

## Methods

`Get(key) (V, bool)`, `Set(key, value)`, `Contains(key) bool`, `Remove(key) bool` (true if it held an unexpired value), `RemovePrefix(prefix)` (empty prefix clears; scans every entry), `Clear()`, `Entries() []Entry` (unexpired, least to most recently used).

Expired entries are invisible at once and reclaimed by the next write after a TTL has passed.

## Keys

A `KeyCodec[K]` turns keys into the strings prefixes match on (and Redis stores, in `redisadapter`). Included: `StringKeyCodec{}` (the key itself), `IntKeyCodec{}` (decimal). For other key types implement `Encode(key K) string` and `Decode(encoded string) (K, error)`. Tested ([example_test.go](../../example_test.go), `ExampleKeyCodec`):

```go
type tenantKey struct {
	Tenant string
	ID     int
}

// tenantKeyCodec encodes a tenantKey as "<tenant>:<id>"; tenants must not contain a colon.
type tenantKeyCodec struct{}

func (tenantKeyCodec) Encode(key tenantKey) string {
	return key.Tenant + ":" + strconv.Itoa(key.ID)
}

func (tenantKeyCodec) Decode(encoded string) (tenantKey, error) {
	tenant, id, found := strings.Cut(encoded, ":")
	if !found {
		return tenantKey{}, fmt.Errorf("tenant key %q has no colon", encoded)
	}
	number, err := strconv.Atoi(id)
	return tenantKey{Tenant: tenant, ID: number}, err
}
```

`Encode` must give distinct keys distinct strings (here: tenants must not contain `:`), `Decode` must invert it. Put the part you remove by prefix first: `RemovePrefix("acme:")` drops one tenant.

## Gotchas

- Values are not copied: a map or slice you change after `Set`, or after `Get`, changes the cached value under every reader.
- `RemovePrefix` is O(n) over all entries.

Source: [local.go](../../local.go), [key.go](../../key.go), [internal/lru](../../internal/lru/lru.go).
