package redisadapter_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/govengers/cache"
	"github.com/govengers/cache/redisadapter"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/goleak"
)

const (
	eventChannel = "events"
	waitDeadline = 5 * time.Second
	pollInterval = 10 * time.Millisecond
)

var errInjected = errors.New("injected failure")

type profile struct {
	Name string
	Age  int
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func newClient(t *testing.T, server *miniredis.Miniredis, hooks ...redis.Hook) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	for _, hook := range hooks {
		client.AddHook(hook)
	}
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client
}

// newStore creates a Store that is closed when the test ends, before its client.
func newStore[K comparable, V any](t *testing.T, options *redisadapter.Options[K]) *redisadapter.Store[K, V] {
	t.Helper()
	store, err := redisadapter.New[K, V](options)
	require.NoError(t, err)
	t.Cleanup(store.Close)
	return store
}

func encode(t *testing.T, value any) string {
	t.Helper()
	encoded, err := msgpack.Marshal(value)
	require.NoError(t, err)
	return string(encoded)
}

// lockedBuffer collects log output written by the receiving goroutine while the test reads it.
type lockedBuffer struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.String()
}

func newLogger() (*slog.Logger, *lockedBuffer) {
	logged := &lockedBuffer{}
	return slog.New(slog.NewTextHandler(logged, nil)), logged
}

// failingHook fails every command named command without sending it to Redis.
type failingHook struct {
	command string
}

func (failingHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h failingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		if command.Name() == h.command {
			command.SetErr(errInjected)
			return errInjected
		}
		return next(ctx, command)
	}
}

func (failingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestNewRejectsInvalidOptions(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:0"})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	valid := func(change func(options *redisadapter.Options[string])) *redisadapter.Options[string] {
		options := &redisadapter.Options[string]{Client: client, KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "items"}
		change(options)
		return options
	}

	testCases := []struct {
		name    string
		options *redisadapter.Options[string]
	}{
		{name: "nil options"},
		{name: "missing client", options: valid(func(options *redisadapter.Options[string]) { options.Client = nil })},
		{name: "missing key codec", options: valid(func(options *redisadapter.Options[string]) { options.KeyCodec = nil })},
		{name: "missing key prefix", options: valid(func(options *redisadapter.Options[string]) { options.KeyPrefix = "" })},
		{name: "negative TTL", options: valid(func(options *redisadapter.Options[string]) { options.TTL = -time.Second })},
		{name: "negative scan count", options: valid(func(options *redisadapter.Options[string]) { options.ScanCount = -1 })},
		{name: "negative heartbeat interval", options: valid(func(options *redisadapter.Options[string]) { options.HeartbeatInterval = -time.Second })},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			store, err := redisadapter.New[string, string](testCase.options)

			require.Error(t, err)
			require.Nil(t, store)
		})
	}
}

func TestStoreSetsGetsAndRemovesUnderItsPrefix(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore[string, profile](t, &redisadapter.Options[string]{
		Client:    newClient(t, server),
		KeyCodec:  cache.StringKeyCodec{},
		KeyPrefix: "users",
	})
	ctx := t.Context()
	alice := profile{Name: "Alice", Age: 42}

	require.NoError(t, store.Set(ctx, "alice", alice))

	value, err := store.Get(ctx, "alice")
	require.NoError(t, err)
	require.Equal(t, alice, value)
	contained, err := store.Contains(ctx, "alice")
	require.NoError(t, err)
	require.True(t, contained)
	stored, err := server.Get("users:alice")
	require.NoError(t, err)
	require.Equal(t, encode(t, alice), stored)

	require.NoError(t, store.Remove(ctx, "alice"))

	_, err = store.Get(ctx, "alice")
	require.ErrorIs(t, err, cache.ErrNotFound)
	contained, err = store.Contains(ctx, "alice")
	require.NoError(t, err)
	require.False(t, contained)
	require.False(t, server.Exists("users:alice"))
}

func TestStoreKeyCodec(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore[int, string](t, &redisadapter.Options[int]{
		Client:    newClient(t, server),
		KeyCodec:  cache.IntKeyCodec{},
		KeyPrefix: "numbers",
	})

	require.Equal(t, cache.KeyCodec[int](cache.IntKeyCodec{}), store.KeyCodec())
}

func TestStoreTTL(t *testing.T) {
	testCases := []struct {
		name     string
		ttl      time.Duration
		expected time.Duration
	}{
		{name: "without TTL the entry never expires", ttl: 0, expected: 0},
		{name: "whole seconds", ttl: 10 * time.Second, expected: 10 * time.Second},
		{name: "below a millisecond rounds up to one", ttl: 500 * time.Microsecond, expected: time.Millisecond},
		{name: "fractions of a millisecond round up", ttl: 1500 * time.Microsecond, expected: 2 * time.Millisecond},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := miniredis.RunT(t)
			store := newStore[string, string](t, &redisadapter.Options[string]{
				Client:    newClient(t, server),
				KeyCodec:  cache.StringKeyCodec{},
				KeyPrefix: "items",
				TTL:       testCase.ttl,
			})
			ctx := t.Context()
			require.NoError(t, store.Set(ctx, "key", "value"))

			remaining, err := store.TTL(ctx, "key")

			require.NoError(t, err)
			require.Equal(t, testCase.expected, remaining)
			server.FastForward(time.Minute)
			contained, err := store.Contains(ctx, "key")
			require.NoError(t, err)
			require.Equal(t, testCase.ttl == 0, contained)
		})
	}
}

func TestStoreTTLOfMissingKeyIsNotFound(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore[string, string](t, &redisadapter.Options[string]{
		Client:    newClient(t, server),
		KeyCodec:  cache.StringKeyCodec{},
		KeyPrefix: "items",
	})

	_, err := store.TTL(t.Context(), "missing")

	require.ErrorIs(t, err, cache.ErrNotFound)
}

type operation struct {
	name    string
	command string
	run     func(ctx context.Context, store *redisadapter.Store[string, string]) error
}

// operations call every Store method that talks to Redis; command is the Redis command each
// one sends last, with "items:alice" stored and an event channel set. Set and Remove run a
// script, which go-redis sends as EVALSHA.
var operations = []operation{
	{name: "get", command: "get", run: func(ctx context.Context, store *redisadapter.Store[string, string]) error {
		_, err := store.Get(ctx, "alice")
		return err
	}},
	{name: "ttl", command: "pttl", run: func(ctx context.Context, store *redisadapter.Store[string, string]) error {
		_, err := store.TTL(ctx, "alice")
		return err
	}},
	{name: "set", command: "evalsha", run: func(ctx context.Context, store *redisadapter.Store[string, string]) error {
		return store.Set(ctx, "alice", "value")
	}},
	{name: "remove", command: "evalsha", run: func(ctx context.Context, store *redisadapter.Store[string, string]) error {
		return store.Remove(ctx, "alice")
	}},
	{name: "remove prefix scanning", command: "scan", run: func(ctx context.Context, store *redisadapter.Store[string, string]) error {
		return store.RemovePrefix(ctx, "a")
	}},
	{name: "remove prefix unlinking", command: "unlink", run: func(ctx context.Context, store *redisadapter.Store[string, string]) error {
		return store.RemovePrefix(ctx, "a")
	}},
	{name: "remove prefix publishing", command: "publish", run: func(ctx context.Context, store *redisadapter.Store[string, string]) error {
		return store.RemovePrefix(ctx, "a")
	}},
	{name: "contains", command: "exists", run: func(ctx context.Context, store *redisadapter.Store[string, string]) error {
		_, err := store.Contains(ctx, "alice")
		return err
	}},
	{name: "load scanning", command: "scan", run: func(ctx context.Context, store *redisadapter.Store[string, string]) error {
		_, err := store.Load(ctx)
		return err
	}},
	{name: "load reading values", command: "mget", run: func(ctx context.Context, store *redisadapter.Store[string, string]) error {
		_, err := store.Load(ctx)
		return err
	}},
}

func newStoreForOperations(t *testing.T, hooks ...redis.Hook) *redisadapter.Store[string, string] {
	t.Helper()
	server := miniredis.RunT(t)
	require.NoError(t, server.Set("items:alice", encode(t, "stored")))
	return newStore[string, string](t, &redisadapter.Options[string]{
		Client:       newClient(t, server, hooks...),
		KeyCodec:     cache.StringKeyCodec{},
		KeyPrefix:    "items",
		EventChannel: eventChannel,
	})
}

func TestStoreHidesRedisErrorChains(t *testing.T) {
	for _, testCase := range operations {
		t.Run(testCase.name, func(t *testing.T) {
			store := newStoreForOperations(t, failingHook{command: testCase.command})

			err := testCase.run(t.Context(), store)

			require.ErrorContains(t, err, errInjected.Error())
			require.NotErrorIs(t, err, errInjected)
			require.NotErrorIs(t, err, cache.ErrNotFound)
		})
	}
}

func TestStoreKeepsContextErrorsMatchable(t *testing.T) {
	for _, testCase := range operations {
		t.Run(testCase.name, func(t *testing.T) {
			store := newStoreForOperations(t)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			err := testCase.run(ctx, store)

			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

func TestStoreContainsReportsUnreachableServer(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), MaxRetries: -1})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	store := newStore[string, string](t, &redisadapter.Options[string]{Client: client, KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "items"})
	server.Close()

	contained, err := store.Contains(t.Context(), "alice")

	require.Error(t, err)
	require.False(t, contained)
}

func TestStoreGetReportsUndecodableValue(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore[string, profile](t, &redisadapter.Options[string]{
		Client:    newClient(t, server),
		KeyCodec:  cache.StringKeyCodec{},
		KeyPrefix: "users",
	})
	require.NoError(t, server.Set("users:alice", "\xc1")) // 0xc1 is never used in MessagePack

	_, err := store.Get(t.Context(), "alice")

	require.ErrorContains(t, err, "redisadapter: decode users:alice")
	require.NotErrorIs(t, err, cache.ErrNotFound)
}

func TestStoreSetReportsUnencodableValue(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore[string, chan int](t, &redisadapter.Options[string]{
		Client:    newClient(t, server),
		KeyCodec:  cache.StringKeyCodec{},
		KeyPrefix: "channels",
	})

	err := store.Set(t.Context(), "alice", make(chan int))

	require.ErrorContains(t, err, "redisadapter: encode channels:alice")
	require.False(t, server.Exists("channels:alice"))
}
