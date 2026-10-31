// Command custombackend implements cache.Backend in memory and runs two Synchronized caches on
// it. It shows the contract a backend for another shared store (Postgres with LISTEN/NOTIFY, NATS
// key-value, ...) must keep:
//
//   - Get, TTL and Contains report a missing key with an error matching cache.ErrNotFound (Contains
//     with false).
//   - Every change reaches every OnEvent handler, the writer's own included, in the order the
//     store applied the changes. Here that holds because the change and its delivery happen
//     under one mutex; across processes the store has to provide it (redisadapter changes the key
//     and publishes the event in one Lua script).
//   - A backend that can lose events (a reconnect, an undecodable message) sends
//     cache.EventReset, so every Synchronized cache drops its local copies. This one cannot.
//   - KeyCodec returns the codec prefixes are matched on.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/govengers/cache"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		slog.Error("example failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, out io.Writer) (err error) {
	backend := newMemoryBackend[int]()
	first, err := cache.NewSynchronized(&cache.SynchronizedOptions[string, int]{Backend: backend})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, first.Close(ctx)) }()
	second, err := cache.NewSynchronized(&cache.SynchronizedOptions[string, int]{Backend: backend})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, second.Close(ctx)) }()

	if err := first.Set(ctx, "visits:home", 1); err != nil {
		return err
	}
	visits, err := second.Get(ctx, "visits:home")
	fmt.Fprintln(out, "second reads", visits, err)
	if err := first.RemovePrefix(ctx, "visits:"); err != nil {
		return err
	}
	_, err = second.Get(ctx, "visits:home")
	fmt.Fprintln(out, "after the prefix removal:", err)
	return nil
}

// memoryBackend is a cache.Backend for string keys whose entries never expire.
type memoryBackend[V any] struct {
	mutex    sync.Mutex
	values   map[string]V
	handlers map[int]func(cache.Event[string, V])
	nextID   int
}

var _ cache.Backend[string, int] = (*memoryBackend[int])(nil)

func newMemoryBackend[V any]() *memoryBackend[V] {
	return &memoryBackend[V]{values: make(map[string]V), handlers: make(map[int]func(cache.Event[string, V]))}
}

func (b *memoryBackend[V]) Get(_ context.Context, key string) (V, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	value, found := b.values[key]
	if !found {
		return value, cache.ErrNotFound
	}
	return value, nil
}

func (b *memoryBackend[V]) TTL(_ context.Context, key string) (time.Duration, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if _, found := b.values[key]; !found {
		return 0, cache.ErrNotFound
	}
	return 0, nil // zero: never expires
}

func (b *memoryBackend[V]) Contains(_ context.Context, key string) (bool, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	_, found := b.values[key]
	return found, nil
}

func (b *memoryBackend[V]) Set(_ context.Context, key string, value V) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.values[key] = value
	b.publish(cache.Event[string, V]{Type: cache.EventSet, Key: key, Value: value})
	return nil
}

func (b *memoryBackend[V]) Remove(_ context.Context, key string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	delete(b.values, key)
	b.publish(cache.Event[string, V]{Type: cache.EventRemove, Key: key})
	return nil
}

func (b *memoryBackend[V]) RemovePrefix(_ context.Context, prefix string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	for key := range b.values {
		if strings.HasPrefix(key, prefix) { // StringKeyCodec: a key is its own encoding
			delete(b.values, key)
		}
	}
	b.publish(cache.Event[string, V]{Type: cache.EventRemovePrefix, Prefix: prefix})
	return nil
}

func (b *memoryBackend[V]) Load(_ context.Context) ([]cache.Entry[string, V], error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	entries := make([]cache.Entry[string, V], 0, len(b.values))
	for key, value := range b.values {
		entries = append(entries, cache.Entry[string, V]{Key: key, Value: value})
	}
	return entries, nil
}

func (b *memoryBackend[V]) KeyCodec() cache.KeyCodec[string] {
	return cache.StringKeyCodec{}
}

func (b *memoryBackend[V]) OnEvent(handler func(cache.Event[string, V])) (unregister func()) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	id := b.nextID
	b.nextID++
	b.handlers[id] = handler
	return func() {
		b.mutex.Lock()
		defer b.mutex.Unlock()
		delete(b.handlers, id)
	}
}

// publish delivers event to every handler while the caller holds the mutex, so handlers see the
// changes in the order they were applied. Handlers must not call back into the backend; the
// handler of a Synchronized cache does not.
func (b *memoryBackend[V]) publish(event cache.Event[string, V]) {
	for _, handler := range b.handlers {
		handler(event)
	}
}
