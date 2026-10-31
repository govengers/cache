// Package lru is a size-bounded least-recently-used map with an optional time to live per entry.
// It is safe for concurrent use and starts no goroutines.
package lru

import (
	"container/list"
	"sync"
	"time"
)

// Entry is a key with its value.
type Entry[K comparable, V any] struct {
	Key   K
	Value V
}

type element[K comparable, V any] struct {
	key       K
	value     V
	expiresAt time.Time // zero: never expires
}

func (e *element[K, V]) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// LRU holds up to size entries and drops the least recently used one when a new key would exceed
// that. With a ttl, an entry expires ttl after it was last written. Expired entries are invisible
// at once and reclaimed by the next write after a ttl has passed since the previous sweep.
type LRU[K comparable, V any] struct {
	mutex       sync.Mutex
	size        int
	ttl         time.Duration
	recency     *list.List // front: most recently used
	elements    map[K]*list.Element
	nextSweepAt time.Time
}

// New returns an LRU with room for size entries (zero: unlimited) whose entries live for ttl
// (zero: forever). Negative values count as zero.
func New[K comparable, V any](size int, ttl time.Duration) *LRU[K, V] {
	return &LRU[K, V]{
		size:     max(size, 0),
		ttl:      max(ttl, 0),
		recency:  list.New(),
		elements: make(map[K]*list.Element),
	}
}

// Add stores value under key, makes it the most recently used entry and restarts its ttl.
func (c *LRU[K, V]) Add(key K, value V) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	now := time.Now()
	c.sweepIfDue(now)
	expiresAt := c.expiryFor(now)

	if existing, found := c.elements[key]; found {
		stored := existing.Value.(*element[K, V])
		stored.value = value
		stored.expiresAt = expiresAt
		c.recency.MoveToFront(existing)
		return
	}

	c.elements[key] = c.recency.PushFront(&element[K, V]{key: key, value: value, expiresAt: expiresAt})
	if c.size > 0 && c.recency.Len() > c.size {
		c.removeElement(c.recency.Back())
	}
}

// Get returns the value of key and makes it the most recently used entry. A missing or expired
// key returns false.
func (c *LRU[K, V]) Get(key K) (V, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	existing, found := c.elements[key]
	if !found || existing.Value.(*element[K, V]).expired(time.Now()) {
		var zero V
		return zero, false
	}
	c.recency.MoveToFront(existing)
	return existing.Value.(*element[K, V]).value, true
}

// Contains reports whether key holds an unexpired value, without changing its recency.
func (c *LRU[K, V]) Contains(key K) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	existing, found := c.elements[key]
	return found && !existing.Value.(*element[K, V]).expired(time.Now())
}

// Remove deletes key and reports whether it held an unexpired value.
func (c *LRU[K, V]) Remove(key K) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	existing, found := c.elements[key]
	if !found {
		return false
	}
	wasLive := !existing.Value.(*element[K, V]).expired(time.Now())
	c.removeElement(existing)
	return wasLive
}

// RemoveFunc deletes every entry whose key matches. match runs under the lock and must not call
// back into the LRU.
func (c *LRU[K, V]) RemoveFunc(match func(key K) bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for current := c.recency.Front(); current != nil; {
		next := current.Next()
		if match(current.Value.(*element[K, V]).key) {
			c.removeElement(current)
		}
		current = next
	}
}

// Clear deletes every entry.
func (c *LRU[K, V]) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.recency.Init()
	clear(c.elements)
}

// Entries returns the unexpired entries from the least to the most recently used, without
// changing their recency.
func (c *LRU[K, V]) Entries() []Entry[K, V] {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	now := time.Now()
	entries := make([]Entry[K, V], 0, c.recency.Len())
	for current := c.recency.Back(); current != nil; current = current.Prev() {
		stored := current.Value.(*element[K, V])
		if !stored.expired(now) {
			entries = append(entries, Entry[K, V]{Key: stored.key, Value: stored.value})
		}
	}
	return entries
}

// Len returns the number of entries held, including expired ones not yet reclaimed.
func (c *LRU[K, V]) Len() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.recency.Len()
}

func (c *LRU[K, V]) expiryFor(now time.Time) time.Time {
	if c.ttl == 0 {
		return time.Time{}
	}
	return now.Add(c.ttl)
}

// sweepIfDue reclaims expired entries at most once per ttl, so an unbounded cache whose keys are
// never read again does not grow forever.
// ponytail: a full scan per ttl, amortized over writes; an expiry-ordered index if writes ever
// become latency-critical on very large caches.
func (c *LRU[K, V]) sweepIfDue(now time.Time) {
	if c.ttl == 0 || now.Before(c.nextSweepAt) {
		return
	}
	c.nextSweepAt = now.Add(c.ttl)
	for current := c.recency.Front(); current != nil; {
		next := current.Next()
		if current.Value.(*element[K, V]).expired(now) {
			c.removeElement(current)
		}
		current = next
	}
}

func (c *LRU[K, V]) removeElement(target *list.Element) {
	c.recency.Remove(target)
	delete(c.elements, target.Value.(*element[K, V]).key)
}
