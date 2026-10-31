package cache

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/govengers/cache/internal/lru"
)

// LocalOptions configures NewLocal.
type LocalOptions[K comparable] struct {
	// KeyCodec encodes keys for RemovePrefix. Required.
	KeyCodec KeyCodec[K]
	// TTL is how long an entry lives after its last write. Zero: entries never expire.
	TTL time.Duration
	// Size is the maximum number of entries; a write beyond it drops the least recently used
	// entry. Zero: unlimited.
	Size int
}

// Local is an in-memory LRU cache with an optional TTL. It is safe for concurrent use and starts
// no goroutines. Values are stored as given, not copied: do not change a value after passing it
// to Set or receiving it from Get.
type Local[K comparable, V any] struct {
	keyCodec KeyCodec[K]
	entries  *lru.LRU[K, V]
}

// NewLocal returns an empty Local cache, or an error if options lacks a KeyCodec or holds a
// negative TTL or Size.
func NewLocal[K comparable, V any](options *LocalOptions[K]) (*Local[K, V], error) {
	if options == nil || options.KeyCodec == nil {
		return nil, errors.New("cache: LocalOptions.KeyCodec is required")
	}
	if options.TTL < 0 || options.Size < 0 {
		return nil, fmt.Errorf("cache: LocalOptions.TTL %s and Size %d must not be negative", options.TTL, options.Size)
	}
	return &Local[K, V]{
		keyCodec: options.KeyCodec,
		entries:  lru.New[K, V](options.Size, options.TTL),
	}, nil
}

// Get returns the value of key, or false if it is missing or expired.
func (c *Local[K, V]) Get(key K) (V, bool) {
	return c.entries.Get(key)
}

// Set stores value under key and restarts its TTL.
func (c *Local[K, V]) Set(key K, value V) {
	c.entries.Add(key, value)
}

// Remove deletes key and reports whether it held an unexpired value.
func (c *Local[K, V]) Remove(key K) bool {
	return c.entries.Remove(key)
}

// RemovePrefix deletes every key whose encoding starts with prefix. An empty prefix clears the
// cache. It scans every entry.
func (c *Local[K, V]) RemovePrefix(prefix string) {
	c.entries.RemoveFunc(func(key K) bool {
		return strings.HasPrefix(c.keyCodec.Encode(key), prefix)
	})
}

// Clear deletes every entry.
func (c *Local[K, V]) Clear() {
	c.entries.Clear()
}

// Contains reports whether key holds an unexpired value.
func (c *Local[K, V]) Contains(key K) bool {
	return c.entries.Contains(key)
}

// Entries returns the unexpired entries from the least to the most recently used.
func (c *Local[K, V]) Entries() []Entry[K, V] {
	stored := c.entries.Entries()
	entries := make([]Entry[K, V], len(stored))
	for index, entry := range stored {
		entries[index] = Entry[K, V](entry)
	}
	return entries
}
