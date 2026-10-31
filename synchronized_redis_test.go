package cache_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/govengers/cache"
	"github.com/govengers/cache/redisadapter"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const eventChannel = "events"

func newRedisClient(t *testing.T, miniRedis *miniredis.Miniredis) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: miniRedis.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client
}

func newStore(t *testing.T, options *redisadapter.Options[string]) *redisadapter.Store[string, string] {
	t.Helper()
	options.KeyCodec = cache.StringKeyCodec{}
	options.KeyPrefix = "test"
	store, err := redisadapter.New[string, string](options)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	return store
}

// newPeer returns a Synchronized cache whose store shares the event channel on miniRedis, once
// the store is subscribed.
func newPeer(t *testing.T, miniRedis *miniredis.Miniredis) *cache.Synchronized[string, string] {
	t.Helper()
	store := newStore(t, &redisadapter.Options[string]{Client: newRedisClient(t, miniRedis), EventChannel: eventChannel})
	synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: store})
	require.NoError(t, store.AwaitSubscription(t.Context()))
	return synchronized
}

func requireValue(t *testing.T, synchronized *cache.Synchronized[string, string], key, value string) {
	t.Helper()
	stored, err := synchronized.Get(t.Context(), key)
	require.NoError(t, err, key)
	require.Equal(t, value, stored, key)
}

func requireEventuallyValue(t *testing.T, synchronized *cache.Synchronized[string, string], key, value string) {
	t.Helper()
	require.Eventually(t, func() bool {
		stored, err := synchronized.Get(context.Background(), key)
		return err == nil && stored == value
	}, 5*time.Second, time.Millisecond, key)
}

func requireEventuallyNotFound(t *testing.T, synchronized *cache.Synchronized[string, string], key string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := synchronized.Get(context.Background(), key)
		return err != nil
	}, 5*time.Second, time.Millisecond, key)
	_, err := synchronized.Get(t.Context(), key)
	require.ErrorIs(t, err, cache.ErrNotFound, key)
}

func TestSynchronizedGetKeepsBackendValuesLocally(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	store := newStore(t, &redisadapter.Options[string]{Client: newRedisClient(t, miniRedis)})
	synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: store})
	require.NoError(t, store.Set(t.Context(), "key", "value"))

	requireValue(t, synchronized, "key", "value")
	miniRedis.FlushAll()
	requireValue(t, synchronized, "key", "value")

	_, err := synchronized.Get(t.Context(), "missing")
	require.ErrorIs(t, err, cache.ErrNotFound)
}

func TestSynchronizedWritesReachBackendAndLocalCache(t *testing.T) {
	for _, asyncWrites := range []bool{false, true} {
		name := "synchronous"
		if asyncWrites {
			name = "asynchronous"
		}
		t.Run(name, func(t *testing.T) {
			miniRedis := miniredis.RunT(t)
			store := newStore(t, &redisadapter.Options[string]{Client: newRedisClient(t, miniRedis)})
			synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: store, AsyncWrites: asyncWrites})
			ctx := t.Context()

			for _, key := range []string{"user:1", "user:2", "user:3", "group:1"} {
				require.NoError(t, synchronized.Set(ctx, key, key+" value"))
			}
			require.NoError(t, synchronized.Remove(ctx, "user:1"))
			require.NoError(t, synchronized.RemovePrefix(ctx, "user:"))
			require.Eventually(t, func() bool {
				return slices.Equal([]string{"test:group:1"}, miniRedis.Keys())
			}, 5*time.Second, time.Millisecond)

			miniRedis.FlushAll()
			requireValue(t, synchronized, "group:1", "group:1 value")
			for _, key := range []string{"user:1", "user:2", "user:3"} {
				_, err := synchronized.Get(ctx, key)
				require.ErrorIs(t, err, cache.ErrNotFound, key)
			}
		})
	}
}

func TestSynchronizedContains(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	store := newStore(t, &redisadapter.Options[string]{Client: newRedisClient(t, miniRedis)})
	synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: store})
	ctx := t.Context()
	require.NoError(t, synchronized.Set(ctx, "local", "value"))
	miniRedis.FlushAll()
	require.NoError(t, store.Set(ctx, "backend", "value"))

	testCases := []struct {
		key      string
		contains bool
	}{
		{key: "local", contains: true},
		{key: "backend", contains: true},
		{key: "missing", contains: false},
	}
	for _, testCase := range testCases {
		t.Run(testCase.key, func(t *testing.T) {
			contains, err := synchronized.Contains(ctx, testCase.key)
			require.NoError(t, err)
			require.Equal(t, testCase.contains, contains)
		})
	}
}

func TestSynchronizedTTL(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	store := newStore(t, &redisadapter.Options[string]{Client: newRedisClient(t, miniRedis), TTL: 10 * time.Second})
	synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: store})
	require.NoError(t, synchronized.Set(t.Context(), "key", "value"))

	remaining, err := synchronized.TTL(t.Context(), "key")
	require.NoError(t, err)
	require.Positive(t, remaining)
	require.LessOrEqual(t, remaining, 10*time.Second)

	_, err = synchronized.TTL(t.Context(), "missing")
	require.ErrorIs(t, err, cache.ErrNotFound)
}

func TestSynchronizedPreloadCopiesTheBackend(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	store := newStore(t, &redisadapter.Options[string]{Client: newRedisClient(t, miniRedis)})
	synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: store})
	require.NoError(t, store.Set(t.Context(), "first", "one"))
	require.NoError(t, store.Set(t.Context(), "second", "two"))

	require.NoError(t, synchronized.Preload(t.Context()))
	miniRedis.FlushAll()

	requireValue(t, synchronized, "first", "one")
	requireValue(t, synchronized, "second", "two")
}

func TestSynchronizedChangesReachPeers(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	writer := newPeer(t, miniRedis)
	reader := newPeer(t, miniRedis)
	ctx := t.Context()

	for _, key := range []string{"user:1", "user:2", "group:1"} {
		require.NoError(t, writer.Set(ctx, key, "old"))
		requireValue(t, reader, key, "old") // now held locally by the reader
	}

	require.NoError(t, writer.Set(ctx, "group:1", "new"))
	requireEventuallyValue(t, reader, "group:1", "new")

	require.NoError(t, writer.Remove(ctx, "user:1"))
	requireEventuallyNotFound(t, reader, "user:1")

	require.NoError(t, writer.RemovePrefix(ctx, "user:"))
	requireEventuallyNotFound(t, reader, "user:2")
	requireValue(t, reader, "group:1", "new")
}

func TestSynchronizedResetsAfterTheEventChannelDropped(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	writer := newPeer(t, miniRedis)
	reader := newPeer(t, miniRedis)
	silentStore := newStore(t, &redisadapter.Options[string]{Client: newRedisClient(t, miniRedis)})
	ctx := t.Context()

	require.NoError(t, writer.Set(ctx, "key", "before"))
	requireValue(t, reader, "key", "before")
	require.NoError(t, silentStore.Set(ctx, "key", "after")) // publishes no event
	requireValue(t, reader, "key", "before")

	miniRedis.Close()
	require.NoError(t, miniRedis.Restart())

	requireEventuallyValue(t, reader, "key", "after")
	requireEventuallyValue(t, writer, "key", "after")
}
