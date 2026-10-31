package cache

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned when a key has no value, either because it was never set, was removed or
// has expired.
var ErrNotFound = errors.New("cache: not found")

// Entry is a key with its value.
type Entry[K comparable, V any] struct {
	Key   K
	Value V
}

// EventType says what changed in a Backend.
type EventType int

// The event types. Their values are part of the wire format of redisadapter and never change.
const (
	// EventSet carries the new value of Event.Key.
	EventSet EventType = iota + 1
	// EventRemove says Event.Key was removed.
	EventRemove
	// EventRemovePrefix says every key whose encoding starts with Event.Prefix was removed.
	EventRemovePrefix
	// EventReset says events may have been lost, so every copy made from earlier events or reads
	// may be stale. redisadapter sends it each time its subscription to the event channel is
	// established.
	EventReset
)

// Event is a change to a Backend, made by this process or by another one sharing the backend.
type Event[K comparable, V any] struct {
	Type   EventType
	Key    K      // EventSet, EventRemove
	Value  V      // EventSet
	Prefix string // EventRemovePrefix
}

// Backend is the shared store behind a Synchronized cache. redisadapter.Store implements it.
// Implementations must be safe for concurrent use.
type Backend[K comparable, V any] interface {
	// Get returns the value of key, or an error matching ErrNotFound.
	Get(ctx context.Context, key K) (V, error)
	// TTL returns the remaining lifetime of key: zero if it never expires, an error matching
	// ErrNotFound if it does not exist.
	TTL(ctx context.Context, key K) (time.Duration, error)
	Set(ctx context.Context, key K, value V) error
	Remove(ctx context.Context, key K) error
	// RemovePrefix removes every key whose encoding starts with prefix.
	RemovePrefix(ctx context.Context, prefix string) error
	Contains(ctx context.Context, key K) (bool, error)
	// Load returns every entry in the backend.
	Load(ctx context.Context) ([]Entry[K, V], error)
	// OnEvent registers handler for every change to the backend, including the caller's own.
	OnEvent(handler func(Event[K, V]))
}
