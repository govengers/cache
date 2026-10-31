package cache

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// ErrClosed is returned by writes of a Synchronized cache with AsyncWrites after Close.
var ErrClosed = errors.New("cache: closed")

const (
	defaultAsyncQueueSize = 1024
	defaultAsyncTimeout   = 10 * time.Second
)

// SynchronizedOptions configures NewSynchronized.
type SynchronizedOptions[K comparable, V any] struct {
	// Backend is the shared store. Required.
	Backend Backend[K, V]
	// LocalTTL is how long a local copy lives after it was read or written. Zero: no expiry.
	// Redis sends no event when a key expires there, so a local copy can outlive the shared
	// entry by up to LocalTTL.
	LocalTTL time.Duration
	// LocalSize is the maximum number of local entries. Zero: unlimited.
	LocalSize int
	// AsyncWrites makes Set, Remove and RemovePrefix change the local cache and return at once.
	// One background goroutine, which NewSynchronized starts and Close stops, then writes to the
	// backend in the order of the calls. A failed or panicking background write is only logged;
	// a failed Set also drops the local copy, a failed removal leaves the backend's value in
	// place.
	AsyncWrites bool
	// AsyncQueueSize is how many background writes may wait for the backend; a write beyond it
	// blocks until there is room or its context ends. Zero: 1024.
	AsyncQueueSize int
	// AsyncTimeout bounds each background write; it replaces the caller's deadline, which has
	// usually passed by then. Zero: 10s.
	AsyncTimeout time.Duration
	// Logger receives failed background writes and panics in them. Nil: nothing is logged.
	Logger *slog.Logger
}

// Synchronized is a local cache in front of a shared Backend. Reads are served locally and fall
// back to the backend; writes go to both. Every process sharing the backend applies the
// backend's events to its local cache, so a change made by one process reaches the others. It is
// safe for concurrent use. Values are stored as given, not copied: do not change a value after
// passing it to Set or receiving it from Get.
type Synchronized[K comparable, V any] struct {
	local      *Local[K, V]
	backend    Backend[K, V]
	logger     *slog.Logger
	unregister func()
	closeOnce  sync.Once
	background *backgroundWriter // nil without AsyncWrites
	fence      *readFence[K, V]
}

// NewSynchronized returns a Synchronized cache with an empty local cache and registers it for
// the backend's events. Call Preload to fill the local cache up front, and Close when done.
func NewSynchronized[K comparable, V any](options *SynchronizedOptions[K, V]) (*Synchronized[K, V], error) {
	if options == nil || options.Backend == nil {
		return nil, errors.New("cache: SynchronizedOptions.Backend is required")
	}
	if options.AsyncQueueSize < 0 || options.AsyncTimeout < 0 {
		return nil, fmt.Errorf("cache: SynchronizedOptions.AsyncQueueSize %d and AsyncTimeout %s must not be negative", options.AsyncQueueSize, options.AsyncTimeout)
	}
	keyCodec := options.Backend.KeyCodec()
	if keyCodec == nil {
		return nil, errors.New("cache: the Backend's KeyCodec is nil")
	}
	local, err := NewLocal[K, V](&LocalOptions[K]{KeyCodec: keyCodec, TTL: options.LocalTTL, Size: options.LocalSize})
	if err != nil {
		return nil, err
	}
	synchronized := &Synchronized[K, V]{
		local:   local,
		backend: options.Backend,
		logger:  cmp.Or(options.Logger, slog.New(slog.DiscardHandler)),
		fence:   newReadFence[K, V](keyCodec),
	}
	if options.AsyncWrites {
		synchronized.background = startBackgroundWriter(
			cmp.Or(options.AsyncQueueSize, defaultAsyncQueueSize),
			cmp.Or(options.AsyncTimeout, defaultAsyncTimeout),
			synchronized.logger,
		)
	}
	synchronized.unregister = options.Backend.OnEvent(synchronized.apply)
	return synchronized, nil
}

// Get returns the value of key, from the local cache or else from the backend, and keeps a value
// read from the backend locally. A key the backend does not hold returns an error matching
// ErrNotFound.
func (c *Synchronized[K, V]) Get(ctx context.Context, key K) (V, error) {
	if value, found := c.local.Get(key); found {
		return value, nil
	}
	ticket := c.fence.beginRead(key)
	value, err := c.backend.Get(ctx, key)
	if err != nil {
		c.fence.finishRead(key, ticket, nil)
		var zero V
		return zero, err
	}
	c.fence.finishRead(key, ticket, func() { c.local.Set(key, value) })
	return value, nil
}

// TTL returns the remaining lifetime of key in the backend: zero if it never expires, an error
// matching ErrNotFound if it does not exist.
func (c *Synchronized[K, V]) TTL(ctx context.Context, key K) (time.Duration, error) {
	return c.backend.TTL(ctx, key)
}

// Set stores value under key in the backend and the local cache. With AsyncWrites it returns
// ErrClosed after Close, or ctx's error when the queue stays full.
func (c *Synchronized[K, V]) Set(ctx context.Context, key K, value V) error {
	if c.background != nil {
		dropLocalCopy := func() { c.fence.changeKey(key, func() { c.local.Remove(key) }) }
		c.fence.changeKey(key, func() { c.local.Set(key, value) })
		err := c.background.enqueue(ctx, "set", func(ctx context.Context) error {
			return c.backend.Set(ctx, key, value)
		}, dropLocalCopy)
		if err != nil {
			dropLocalCopy() // the backend never gets the value, so this process must not serve it
		}
		return err
	}
	ticket := c.fence.beginRead(key)
	if err := c.backend.Set(ctx, key, value); err != nil {
		c.fence.finishRead(key, ticket, nil)
		return err
	}
	// A change applied meanwhile may be newer than this write; its event, or the next read,
	// brings the local cache up to date.
	c.fence.finishRead(key, ticket, func() { c.local.Set(key, value) })
	return nil
}

// Remove deletes key from the backend and the local cache. With AsyncWrites it returns ErrClosed
// after Close, or ctx's error when the queue stays full.
func (c *Synchronized[K, V]) Remove(ctx context.Context, key K) error {
	removeLocally := func() { c.fence.changeKey(key, func() { c.local.Remove(key) }) }
	return c.removeEverywhere(ctx, "remove", removeLocally, func(ctx context.Context) error {
		return c.backend.Remove(ctx, key)
	})
}

// RemovePrefix deletes every key whose encoding starts with prefix from the backend and the local
// cache. With AsyncWrites it returns ErrClosed after Close, or ctx's error when the queue stays
// full.
func (c *Synchronized[K, V]) RemovePrefix(ctx context.Context, prefix string) error {
	removeLocally := func() { c.fence.changePrefix(prefix, func() { c.local.RemovePrefix(prefix) }) }
	return c.removeEverywhere(ctx, "remove prefix", removeLocally, func(ctx context.Context) error {
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

// Preload copies every entry of the backend into the local cache, except those whose key
// changed while it ran; they are read on demand later. A later EventReset drops the entries
// again; with redisadapter, call Store.AwaitSubscription first.
func (c *Synchronized[K, V]) Preload(ctx context.Context) error {
	tracker := c.fence.beginLoad()
	entries, err := c.backend.Load(ctx)
	if err != nil {
		c.fence.finishLoad(tracker, nil, nil)
		return err
	}
	c.fence.finishLoad(tracker, entries, c.local.Set)
	return nil
}

// Close stops applying the backend's events and, with AsyncWrites, waits until every queued
// background write has finished, or returns ctx's error. Later writes with AsyncWrites return
// ErrClosed. Close does not close the backend. It is safe to call more than once.
func (c *Synchronized[K, V]) Close(ctx context.Context) error {
	c.closeOnce.Do(c.unregister)
	if c.background == nil {
		return nil
	}
	return c.background.close(ctx)
}

func (c *Synchronized[K, V]) apply(event Event[K, V]) {
	switch event.Type {
	case EventSet:
		c.fence.changeKey(event.Key, func() { c.local.Set(event.Key, event.Value) })
	case EventRemove:
		c.fence.changeKey(event.Key, func() { c.local.Remove(event.Key) })
	case EventRemovePrefix:
		c.fence.changePrefix(event.Prefix, func() { c.local.RemovePrefix(event.Prefix) })
	case EventReset:
		c.fence.changePrefix("", c.local.Clear)
	}
}

func (c *Synchronized[K, V]) removeEverywhere(ctx context.Context, operation string, removeLocally func(), removeFromBackend func(ctx context.Context) error) error {
	if c.background != nil {
		removeLocally()
		return c.background.enqueue(ctx, operation, removeFromBackend, nil)
	}
	if err := removeFromBackend(ctx); err != nil {
		return err
	}
	removeLocally()
	return nil
}
