package cache

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/govengers/cache/internal/recovery"
)

// SynchronizedOptions configures NewSynchronized.
type SynchronizedOptions[K comparable, V any] struct {
	// Backend is the shared store. Required.
	Backend Backend[K, V]
	// KeyCodec must encode keys exactly like the backend does, so prefix removals match the same
	// keys locally and in the backend. Required.
	KeyCodec KeyCodec[K]
	// LocalTTL is how long a local copy lives after its last write. Zero: no expiry. Redis sends
	// no event when a key expires there, so keep LocalTTL at most the backend's TTL, or a local
	// copy outlives the shared entry.
	LocalTTL time.Duration
	// LocalSize is the maximum number of local entries. Zero: unlimited.
	LocalSize int
	// AsyncWrites makes Set, Remove and RemovePrefix change the local cache and return at once,
	// writing to the backend in the background. A failed or panicking background write is only
	// logged; a failed Set also drops the local copy, a failed removal leaves the backend's value
	// in place.
	AsyncWrites bool
	// Logger receives failed background writes and panics in them. Nil: nothing is logged.
	Logger *slog.Logger
}

// Synchronized is a local cache in front of a shared Backend. Reads are served locally and fall
// back to the backend; writes go to both. Every process sharing the backend applies the
// backend's events to its local cache, so a change made by one process reaches the others. It is
// safe for concurrent use.
type Synchronized[K comparable, V any] struct {
	local       *Local[K, V]
	backend     Backend[K, V]
	asyncWrites bool
	logger      *slog.Logger
	pending     sync.WaitGroup

	// changeMutex and generation keep a value read from the backend from overwriting a change
	// that was applied locally while the read was in flight.
	changeMutex sync.Mutex
	generation  uint64
}

// NewSynchronized returns a Synchronized cache with an empty local cache and registers it for
// the backend's events. Call Preload to fill the local cache up front.
func NewSynchronized[K comparable, V any](options *SynchronizedOptions[K, V]) (*Synchronized[K, V], error) {
	if options == nil || options.Backend == nil {
		return nil, errors.New("cache: SynchronizedOptions.Backend is required")
	}
	local, err := NewLocal[K, V](&LocalOptions[K]{KeyCodec: options.KeyCodec, TTL: options.LocalTTL, Size: options.LocalSize})
	if err != nil {
		return nil, err
	}
	synchronized := &Synchronized[K, V]{
		local:       local,
		backend:     options.Backend,
		asyncWrites: options.AsyncWrites,
		logger:      cmp.Or(options.Logger, slog.New(slog.DiscardHandler)),
	}
	options.Backend.OnEvent(synchronized.apply)
	return synchronized, nil
}

// Get returns the value of key, from the local cache or else from the backend, and keeps a value
// read from the backend locally. A key the backend does not hold returns an error matching
// ErrNotFound.
func (c *Synchronized[K, V]) Get(ctx context.Context, key K) (V, error) {
	if value, found := c.local.Get(key); found {
		return value, nil
	}
	generation := c.currentGeneration()
	value, err := c.backend.Get(ctx, key)
	if err != nil {
		var zero V
		return zero, err
	}
	c.fill(generation, key, value)
	return value, nil
}

// TTL returns the remaining lifetime of key in the backend: zero if it never expires, an error
// matching ErrNotFound if it does not exist.
func (c *Synchronized[K, V]) TTL(ctx context.Context, key K) (time.Duration, error) {
	return c.backend.TTL(ctx, key)
}

// Set stores value under key in the backend and the local cache.
func (c *Synchronized[K, V]) Set(ctx context.Context, key K, value V) error {
	if c.asyncWrites {
		c.change(func() { c.local.Set(key, value) })
		c.writeInBackground(ctx, "set", func(ctx context.Context) error {
			return c.backend.Set(ctx, key, value)
		}, func() {
			c.change(func() { c.local.Remove(key) })
		})
		return nil
	}
	if err := c.backend.Set(ctx, key, value); err != nil {
		return err
	}
	c.change(func() { c.local.Set(key, value) })
	return nil
}

// Remove deletes key from the backend and the local cache.
func (c *Synchronized[K, V]) Remove(ctx context.Context, key K) error {
	return c.removeEverywhere(ctx, "remove", func() { c.local.Remove(key) }, func(ctx context.Context) error {
		return c.backend.Remove(ctx, key)
	})
}

// RemovePrefix deletes every key whose encoding starts with prefix from the backend and the local
// cache.
func (c *Synchronized[K, V]) RemovePrefix(ctx context.Context, prefix string) error {
	return c.removeEverywhere(ctx, "remove prefix", func() { c.local.RemovePrefix(prefix) }, func(ctx context.Context) error {
		return c.backend.RemovePrefix(ctx, prefix)
	})
}

// Contains reports whether key holds a value locally or in the backend.
func (c *Synchronized[K, V]) Contains(ctx context.Context, key K) (bool, error) {
	if c.local.Contains(key) {
		return true, nil
	}
	return c.backend.Contains(ctx, key)
}

// Preload copies every entry of the backend into the local cache. Entries that arrive while a
// concurrent change is applied are skipped and read on demand later. A later EventReset drops
// them again; with redisadapter, call Store.AwaitSubscription first.
func (c *Synchronized[K, V]) Preload(ctx context.Context) error {
	generation := c.currentGeneration()
	entries, err := c.backend.Load(ctx)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		c.fill(generation, entry.Key, entry.Value)
	}
	return nil
}

// Close waits for the background writes of AsyncWrites to finish. Call it after the last write;
// it does not stop the backend.
func (c *Synchronized[K, V]) Close() {
	c.pending.Wait()
}

func (c *Synchronized[K, V]) apply(event Event[K, V]) {
	switch event.Type {
	case EventSet:
		c.change(func() { c.local.Set(event.Key, event.Value) })
	case EventRemove:
		c.change(func() { c.local.Remove(event.Key) })
	case EventRemovePrefix:
		c.change(func() { c.local.RemovePrefix(event.Prefix) })
	case EventReset:
		c.change(func() { c.local.RemovePrefix("") })
	}
}

func (c *Synchronized[K, V]) removeEverywhere(ctx context.Context, operation string, removeLocally func(), removeFromBackend func(ctx context.Context) error) error {
	if c.asyncWrites {
		c.change(removeLocally)
		c.writeInBackground(ctx, operation, removeFromBackend, nil)
		return nil
	}
	if err := removeFromBackend(ctx); err != nil {
		return err
	}
	c.change(removeLocally)
	return nil
}

// writeInBackground runs write detached from the caller's cancellation, so it completes after
// the caller returned, and runs onFailure (if any) when it fails or panics.
func (c *Synchronized[K, V]) writeInBackground(ctx context.Context, operation string, write func(ctx context.Context) error, onFailure func()) {
	detached := context.WithoutCancel(ctx)
	c.pending.Go(func() {
		succeeded := false
		recovery.Run(c.logger, "cache: background "+operation, func() {
			if err := write(detached); err != nil {
				c.logger.Error("cache: background "+operation+" failed", "error", err)
				return
			}
			succeeded = true
		})
		if !succeeded && onFailure != nil {
			onFailure()
		}
	})
}

func (c *Synchronized[K, V]) change(changeLocally func()) {
	c.changeMutex.Lock()
	defer c.changeMutex.Unlock()
	c.generation++
	changeLocally()
}

func (c *Synchronized[K, V]) currentGeneration() uint64 {
	c.changeMutex.Lock()
	defer c.changeMutex.Unlock()
	return c.generation
}

// fill keeps a value read from the backend unless a change was applied since generation.
func (c *Synchronized[K, V]) fill(generation uint64, key K, value V) {
	c.changeMutex.Lock()
	defer c.changeMutex.Unlock()
	if c.generation == generation {
		c.local.Set(key, value)
	}
}
