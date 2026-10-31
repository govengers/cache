// Package redisadapter stores cache entries in Redis and shares their changes between processes
// through a Redis Pub/Sub channel. Its Store is the cache.Backend of a cache.Synchronized.
package redisadapter

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/govengers/cache"
	"github.com/govengers/cache/internal/opaque"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

// StoreOptions configures NewStore.
type StoreOptions[K comparable] struct {
	// Client is the Redis connection: redis.NewClient for one server, redis.NewFailoverClient
	// behind Sentinel. Redis Cluster is not supported. The caller creates it (and instruments it,
	// for example with redisotel), and closes it after Store.Close. Required.
	Client *redis.Client
	// KeyCodec encodes keys into Redis keys. Required.
	KeyCodec cache.KeyCodec[K]
	// KeyPrefix namespaces the keys: a key is stored as "<KeyPrefix>:<encoded key>". Load and
	// RemovePrefix only see keys in the namespace, so give every Store sharing a database its
	// own prefix. Empty: keys are stored as they are encoded, and Load reads the whole database.
	KeyPrefix string
	// TTL is how long Redis keeps an entry after its last write. Zero: forever.
	TTL time.Duration
	// EventChannel is the Pub/Sub channel this store publishes its changes on and receives the
	// changes of every store sharing it from. NewStore then starts a goroutine that Close stops.
	// Events are delivered at most once: changes made while no subscription is up are missed, so
	// handlers receive cache.EventReset each time the subscription is established, the first
	// time included. Empty: no events.
	EventChannel string
	// HeartbeatInterval is how often the store checks that its subscription still receives what
	// is published on the primary. A subscription that stays on a former primary after a
	// Sentinel failover, or on a dead connection, is renewed within three intervals. Zero: 5s.
	HeartbeatInterval time.Duration
	// ScanCount is the COUNT hint of the SCAN calls of Load and RemovePrefix. Zero: the Redis
	// default.
	ScanCount int64
	// Logger receives what cannot be returned: dropped subscriptions, undecodable events and
	// entries, panics in event handlers. Nil: nothing is logged.
	Logger *slog.Logger
}

const defaultHeartbeatInterval = 5 * time.Second

// Store is a cache.Backend in Redis. Values are encoded with MessagePack. It is safe for
// concurrent use.
type Store[K comparable, V any] struct {
	client            *redis.Client
	keyCodec          cache.KeyCodec[K]
	namespace         string
	ttl               time.Duration
	eventChannel      string
	heartbeatInterval time.Duration
	scanCount         int64
	logger            *slog.Logger

	handlersMutex sync.RWMutex
	handlers      []func(cache.Event[K, V])

	stopReceiving     context.CancelFunc
	receiving         sync.WaitGroup
	subscribed        chan struct{}
	firstSubscription sync.Once
}

// NewStore returns a Store, and starts receiving events if options.EventChannel is set. It
// returns an error if Client or KeyCodec is missing or TTL, ScanCount or HeartbeatInterval is
// negative.
func NewStore[K comparable, V any](options *StoreOptions[K]) (*Store[K, V], error) {
	if err := validate(options); err != nil {
		return nil, err
	}
	store := &Store[K, V]{
		client:            options.Client,
		keyCodec:          options.KeyCodec,
		namespace:         namespace(options.KeyPrefix),
		ttl:               options.TTL,
		eventChannel:      options.EventChannel,
		heartbeatInterval: cmp.Or(options.HeartbeatInterval, defaultHeartbeatInterval),
		scanCount:         options.ScanCount,
		logger:            cmp.Or(options.Logger, slog.New(slog.DiscardHandler)),
		stopReceiving:     func() {},
		subscribed:        make(chan struct{}),
	}
	if store.eventChannel != "" {
		store.startReceiving()
	}
	return store, nil
}

func validate[K comparable](options *StoreOptions[K]) error {
	if options == nil || options.Client == nil || options.KeyCodec == nil {
		return errors.New("redisadapter: StoreOptions.Client and KeyCodec are required")
	}
	if options.TTL < 0 || options.ScanCount < 0 || options.HeartbeatInterval < 0 {
		return fmt.Errorf("redisadapter: StoreOptions.TTL %s, ScanCount %d and HeartbeatInterval %s must not be negative", options.TTL, options.ScanCount, options.HeartbeatInterval)
	}
	return nil
}

func namespace(keyPrefix string) string {
	if keyPrefix == "" {
		return ""
	}
	return keyPrefix + ":"
}

// Get returns the value of key, or an error matching cache.ErrNotFound.
func (s *Store[K, V]) Get(ctx context.Context, key K) (V, error) {
	var value V
	encoded, err := s.client.Get(ctx, s.redisKey(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return value, fmt.Errorf("redisadapter: get %s: %w", s.redisKey(key), cache.ErrNotFound)
	}
	if err != nil {
		return value, opaque.Error("redisadapter: get "+s.redisKey(key), err)
	}
	if err := msgpack.Unmarshal(encoded, &value); err != nil {
		return value, opaque.Error("redisadapter: decode "+s.redisKey(key), err)
	}
	return value, nil
}

// TTL returns the remaining lifetime of key: zero if it never expires, an error matching
// cache.ErrNotFound if it does not exist.
func (s *Store[K, V]) TTL(ctx context.Context, key K) (time.Duration, error) {
	remaining, err := s.client.PTTL(ctx, s.redisKey(key)).Result()
	if err != nil {
		return 0, opaque.Error("redisadapter: ttl "+s.redisKey(key), err)
	}
	return lifetime(s.redisKey(key), remaining)
}

// go-redis passes the PTTL replies for a missing key (-2) and a key without expiry (-1) through
// as nanosecond durations.
const (
	pttlMissingKey  = -2 * time.Nanosecond
	pttlNeverExpire = -1 * time.Nanosecond
)

func lifetime(redisKey string, remaining time.Duration) (time.Duration, error) {
	switch remaining {
	case pttlMissingKey:
		return 0, fmt.Errorf("redisadapter: ttl %s: %w", redisKey, cache.ErrNotFound)
	case pttlNeverExpire:
		return 0, nil
	default:
		return remaining, nil
	}
}

// Set stores value under key with the store's TTL and publishes the change.
func (s *Store[K, V]) Set(ctx context.Context, key K, value V) error {
	encoded, err := msgpack.Marshal(value)
	if err != nil {
		return opaque.Error("redisadapter: encode "+s.redisKey(key), err)
	}
	if err := s.client.Set(ctx, s.redisKey(key), encoded, s.ttl).Err(); err != nil {
		return opaque.Error("redisadapter: set "+s.redisKey(key), err)
	}
	return s.publish(ctx, &eventMessage[K, V]{Type: cache.EventSet, Key: key, Value: value})
}

// Remove deletes key and publishes the change.
func (s *Store[K, V]) Remove(ctx context.Context, key K) error {
	if err := s.client.Del(ctx, s.redisKey(key)).Err(); err != nil {
		return opaque.Error("redisadapter: remove "+s.redisKey(key), err)
	}
	return s.publish(ctx, &eventMessage[K, V]{Type: cache.EventRemove, Key: key})
}

// RemovePrefix deletes every key whose encoding starts with prefix and publishes the change. An
// empty prefix clears the namespace. Keys written while it runs may survive.
func (s *Store[K, V]) RemovePrefix(ctx context.Context, prefix string) error {
	err := s.scan(ctx, prefix, func(redisKeys []string) error {
		return s.client.Unlink(ctx, redisKeys...).Err()
	})
	if err != nil {
		return opaque.Error("redisadapter: remove prefix "+s.namespace+prefix, err)
	}
	return s.publish(ctx, &eventMessage[K, V]{Type: cache.EventRemovePrefix, Prefix: prefix})
}

// Contains reports whether key exists.
func (s *Store[K, V]) Contains(ctx context.Context, key K) (bool, error) {
	count, err := s.client.Exists(ctx, s.redisKey(key)).Result()
	if err != nil {
		return false, opaque.Error("redisadapter: contains "+s.redisKey(key), err)
	}
	return count == 1, nil
}

// OnEvent registers handler for every change published on the event channel, including this
// store's own. Handlers run one at a time on the receiving goroutine and must not block; a
// panicking handler is logged and skipped. Without an event channel, handlers are never called.
func (s *Store[K, V]) OnEvent(handler func(cache.Event[K, V])) {
	s.handlersMutex.Lock()
	defer s.handlersMutex.Unlock()
	s.handlers = append(s.handlers, handler)
}

// ErrNoEventChannel is returned by AwaitSubscription on a store without an EventChannel.
var ErrNoEventChannel = errors.New("redisadapter: the store has no EventChannel")

// AwaitSubscription returns once the store receives events, or with ctx's error. Call it before
// cache.Synchronized.Preload: the EventReset of the first subscription would drop what was
// preloaded before it. It returns ErrNoEventChannel without an EventChannel.
func (s *Store[K, V]) AwaitSubscription(ctx context.Context) error {
	if s.eventChannel == "" {
		return ErrNoEventChannel
	}
	select {
	case <-s.subscribed:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("redisadapter: await subscription to %s: %w", s.eventChannel, ctx.Err())
	}
}

// Close stops receiving events and waits for the receiving goroutine to exit. It does not close
// the Redis client. It is safe to call more than once.
func (s *Store[K, V]) Close() {
	s.stopReceiving()
	s.receiving.Wait()
}

func (s *Store[K, V]) redisKey(key K) string {
	return s.namespace + s.keyCodec.Encode(key)
}
