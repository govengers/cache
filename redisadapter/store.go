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

// Options configures New.
type Options[K comparable] struct {
	// Client is the Redis connection: redis.NewClient for one server, redis.NewFailoverClient
	// behind Sentinel. Redis Cluster is not supported. The caller creates it (and instruments it,
	// for example with redisotel), and closes it after Store.Close. Required.
	Client *redis.Client
	// KeyCodec encodes keys into Redis keys. Required.
	KeyCodec cache.KeyCodec[K]
	// KeyPrefix namespaces the keys: a key is stored as "<KeyPrefix>:<encoded key>". Load and
	// RemovePrefix only see keys in the namespace, so give every Store sharing a database its
	// own prefix. Required, so that clearing a cache never clears the database.
	KeyPrefix string
	// TTL is how long Redis keeps an entry after its last write, rounded up to whole
	// milliseconds. Zero: forever.
	TTL time.Duration
	// EventChannel is the Pub/Sub channel this store publishes its changes on and receives the
	// changes of every store sharing it from. New then starts a goroutine that Close stops.
	// Events are delivered at most once: changes made while no subscription is up are missed, so
	// handlers receive cache.EventReset each time the subscription is established, the first
	// time included. Empty: no events.
	EventChannel string
	// HeartbeatInterval is how often the store checks that its subscription still receives what
	// is published on the primary, and that the primary is still the same server. A
	// subscription left on a former primary after a Sentinel failover is renewed at the next
	// heartbeat, one on a dead connection after three to four intervals. Each heartbeat is one
	// PUBLISH and one INFO on the primary. Zero: 5s.
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

	handlersMutex sync.Mutex
	handlers      []registeredHandler[K, V]
	nextHandlerID uint64

	stopReceiving     context.CancelFunc
	receiving         sync.WaitGroup
	subscribed        chan struct{}
	firstSubscription sync.Once
}

// New returns a Store, and starts receiving events if options.EventChannel is set. It
// returns an error if Client, KeyCodec or KeyPrefix is missing or TTL, ScanCount or HeartbeatInterval is
// negative.
func New[K comparable, V any](options *Options[K]) (*Store[K, V], error) {
	if err := validate(options); err != nil {
		return nil, err
	}
	store := &Store[K, V]{
		client:            options.Client,
		keyCodec:          options.KeyCodec,
		namespace:         options.KeyPrefix + ":",
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

func validate[K comparable](options *Options[K]) error {
	if options == nil || options.Client == nil || options.KeyCodec == nil || options.KeyPrefix == "" {
		return errors.New("redisadapter: Options.Client, KeyCodec and KeyPrefix are required")
	}
	if options.TTL < 0 || options.ScanCount < 0 || options.HeartbeatInterval < 0 {
		return fmt.Errorf("redisadapter: Options.TTL %s, ScanCount %d and HeartbeatInterval %s must not be negative", options.TTL, options.ScanCount, options.HeartbeatInterval)
	}
	return nil
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

// The mutations publish their event in the same script, so events reach subscribers in the
// order Redis applied the changes, and no change is applied without its event. An empty
// channel publishes nothing.
var (
	// KEYS: key. ARGV: value, ttl in milliseconds (0: none), channel, event.
	setScript = redis.NewScript(`
if ARGV[2] == '0' then
	redis.call('SET', KEYS[1], ARGV[1])
else
	redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
end
if ARGV[3] ~= '' then
	redis.call('PUBLISH', ARGV[3], ARGV[4])
end
return 1
`)

	// KEYS: key. ARGV: channel, event.
	removeScript = redis.NewScript(`
redis.call('DEL', KEYS[1])
if ARGV[1] ~= '' then
	redis.call('PUBLISH', ARGV[1], ARGV[2])
end
return 1
`)
)

// Set stores value under key with the store's TTL and publishes the change.
func (s *Store[K, V]) Set(ctx context.Context, key K, value V) error {
	encodedValue, err := msgpack.Marshal(value)
	if err != nil {
		return opaque.Error("redisadapter: encode "+s.redisKey(key), err)
	}
	event, err := encodeEvent(&eventMessage{Type: cache.EventSet, Key: s.keyCodec.Encode(key), Value: encodedValue})
	if err != nil {
		return opaque.Error("redisadapter: set "+s.redisKey(key), err)
	}
	arguments := []any{encodedValue, s.ttlMilliseconds(), s.eventChannel, event}
	if err := setScript.Run(ctx, s.client, []string{s.redisKey(key)}, arguments...).Err(); err != nil {
		return opaque.Error("redisadapter: set "+s.redisKey(key), err)
	}
	return nil
}

// ttlMilliseconds rounds the TTL up to whole milliseconds, Redis's precision, so an entry never
// expires before its TTL; zero stays zero, no expiry.
func (s *Store[K, V]) ttlMilliseconds() int64 {
	return int64((s.ttl + time.Millisecond - 1) / time.Millisecond)
}

// Remove deletes key and publishes the change.
func (s *Store[K, V]) Remove(ctx context.Context, key K) error {
	event, err := encodeEvent(&eventMessage{Type: cache.EventRemove, Key: s.keyCodec.Encode(key)})
	if err != nil {
		return opaque.Error("redisadapter: remove "+s.redisKey(key), err)
	}
	if err := removeScript.Run(ctx, s.client, []string{s.redisKey(key)}, s.eventChannel, event).Err(); err != nil {
		return opaque.Error("redisadapter: remove "+s.redisKey(key), err)
	}
	return nil
}

// RemovePrefix deletes every key whose encoding starts with prefix and publishes the change. An
// empty prefix clears the namespace. Keys written while it runs may survive. The change is
// published even when the removal fails part way, because some keys may be gone already.
func (s *Store[K, V]) RemovePrefix(ctx context.Context, prefix string) error {
	removeError := s.scan(ctx, prefix, func(redisKeys []string) error {
		return s.client.Unlink(ctx, redisKeys...).Err()
	})
	if removeError != nil {
		removeError = opaque.Error("redisadapter: remove prefix "+s.namespace+prefix, removeError)
	}
	return errors.Join(removeError, s.publish(ctx, &eventMessage{Type: cache.EventRemovePrefix, Prefix: prefix}))
}

func (s *Store[K, V]) publish(ctx context.Context, message *eventMessage) error {
	if s.eventChannel == "" {
		return nil
	}
	event, err := encodeEvent(message)
	if err != nil {
		return opaque.Error("redisadapter: publish on "+s.eventChannel, err)
	}
	if err := s.client.Publish(ctx, s.eventChannel, event).Err(); err != nil {
		return opaque.Error("redisadapter: publish on "+s.eventChannel, err)
	}
	return nil
}

// Contains reports whether key exists.
func (s *Store[K, V]) Contains(ctx context.Context, key K) (bool, error) {
	count, err := s.client.Exists(ctx, s.redisKey(key)).Result()
	if err != nil {
		return false, opaque.Error("redisadapter: contains "+s.redisKey(key), err)
	}
	return count == 1, nil
}

// ErrNoEventChannel is returned by AwaitSubscription on a store without an EventChannel.
var ErrNoEventChannel = errors.New("redisadapter: the store has no EventChannel")

// AwaitSubscription returns once the store's first subscription to the event channel was
// established, or with ctx's error. It does not report later reconnects, which deliver their own
// cache.EventReset. Call it before cache.Synchronized.Preload: the EventReset of the first
// subscription would drop what was preloaded before it. It returns ErrNoEventChannel without an
// EventChannel.
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

// KeyCodec returns the codec of Options.KeyCodec.
func (s *Store[K, V]) KeyCodec() cache.KeyCodec[K] {
	return s.keyCodec
}

func (s *Store[K, V]) redisKey(key K) string {
	return s.namespace + s.keyCodec.Encode(key)
}
