package cache_test

import (
	"testing"
	"time"

	"github.com/govengers/cache"
	"github.com/stretchr/testify/require"
)

func newLocal[K comparable, V any](t *testing.T, options *cache.LocalOptions[K]) *cache.Local[K, V] {
	t.Helper()
	local, err := cache.NewLocal[K, V](options)
	require.NoError(t, err)
	return local
}

func TestNewLocalRejectsInvalidOptions(t *testing.T) {
	testCases := []struct {
		name    string
		options *cache.LocalOptions[string]
	}{
		{name: "nil options", options: nil},
		{name: "missing KeyCodec", options: &cache.LocalOptions[string]{}},
		{name: "negative TTL", options: &cache.LocalOptions[string]{KeyCodec: cache.StringKeyCodec{}, TTL: -time.Second}},
		{name: "negative Size", options: &cache.LocalOptions[string]{KeyCodec: cache.StringKeyCodec{}, Size: -1}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			local, err := cache.NewLocal[string, string](testCase.options)
			require.Error(t, err)
			require.Nil(t, local)
		})
	}
}

func TestLocalSetGetRemoveContains(t *testing.T) {
	type profile struct {
		Name string
	}
	local := newLocal[int, profile](t, &cache.LocalOptions[int]{KeyCodec: cache.IntKeyCodec{}})

	_, found := local.Get(1)
	require.False(t, found)
	require.False(t, local.Contains(1))

	local.Set(1, profile{Name: "first"})
	value, found := local.Get(1)
	require.True(t, found)
	require.Equal(t, profile{Name: "first"}, value)
	require.True(t, local.Contains(1))

	require.True(t, local.Remove(1))
	require.False(t, local.Remove(1))
	_, found = local.Get(1)
	require.False(t, found)
	require.False(t, local.Contains(1))
}

func TestLocalRemovePrefix(t *testing.T) {
	testCases := []struct {
		name      string
		prefix    string
		remaining []string
	}{
		{name: "matching prefix", prefix: "user:", remaining: []string{"group:1", "username"}},
		{name: "prefix without separator", prefix: "user", remaining: []string{"group:1"}},
		{name: "no match", prefix: "team:", remaining: []string{"user:1", "group:1", "user:2", "username"}},
		{name: "empty prefix clears the cache", prefix: "", remaining: []string{}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			local := newLocal[string, int](t, &cache.LocalOptions[string]{KeyCodec: cache.StringKeyCodec{}})
			for index, key := range []string{"user:1", "group:1", "user:2", "username"} {
				local.Set(key, index)
			}

			local.RemovePrefix(testCase.prefix)

			remaining := []string{}
			for _, entry := range local.Entries() {
				remaining = append(remaining, entry.Key)
			}
			require.Equal(t, testCase.remaining, remaining)
		})
	}
}

func TestLocalRemovePrefixMatchesTheEncodedKey(t *testing.T) {
	local := newLocal[int, string](t, &cache.LocalOptions[int]{KeyCodec: cache.IntKeyCodec{}})
	local.Set(1, "one")
	local.Set(12, "twelve")
	local.Set(2, "two")

	local.RemovePrefix("1")

	require.Equal(t, []cache.Entry[int, string]{{Key: 2, Value: "two"}}, local.Entries())
}

func TestLocalEntriesFromLeastToMostRecentlyUsed(t *testing.T) {
	local := newLocal[string, int](t, &cache.LocalOptions[string]{KeyCodec: cache.StringKeyCodec{}})
	require.Empty(t, local.Entries())

	local.Set("first", 1)
	local.Set("second", 2)
	local.Set("third", 3)
	local.Get("first")

	require.Equal(t, []cache.Entry[string, int]{
		{Key: "second", Value: 2},
		{Key: "third", Value: 3},
		{Key: "first", Value: 1},
	}, local.Entries())
}

func TestLocalSizeDropsTheLeastRecentlyUsedEntry(t *testing.T) {
	local := newLocal[string, int](t, &cache.LocalOptions[string]{KeyCodec: cache.StringKeyCodec{}, Size: 2})
	local.Set("first", 1)
	local.Set("second", 2)
	local.Get("first")

	local.Set("third", 3)

	require.False(t, local.Contains("second"))
	require.True(t, local.Contains("first"))
	require.True(t, local.Contains("third"))
}

func TestLocalClearDeletesEveryEntry(t *testing.T) {
	local := newLocal[string, int](t, &cache.LocalOptions[string]{KeyCodec: cache.StringKeyCodec{}})
	local.Set("first", 1)
	local.Set("second", 2)

	local.Clear()

	require.Empty(t, local.Entries())
	require.False(t, local.Contains("first"))
	local.Set("third", 3)
	require.Equal(t, []cache.Entry[string, int]{{Key: "third", Value: 3}}, local.Entries())
}

func TestLocalTTLExpiresEntries(t *testing.T) {
	local := newLocal[string, int](t, &cache.LocalOptions[string]{KeyCodec: cache.StringKeyCodec{}, TTL: 5 * time.Millisecond})
	local.Set("key", 1)

	require.Eventually(t, func() bool {
		_, found := local.Get("key")
		return !found
	}, time.Second, time.Millisecond)
	require.False(t, local.Contains("key"))
	require.Empty(t, local.Entries())
	require.False(t, local.Remove("key"))
}
