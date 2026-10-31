package lru

import (
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// expire moves the expiry of key into the past, so a test sees an expired entry without waiting.
func expire[K comparable, V any](t *testing.T, cache *LRU[K, V], key K) {
	t.Helper()
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	existing, found := cache.elements[key]
	require.True(t, found, "key %v is not stored", key)
	existing.Value.(*element[K, V]).expiresAt = time.Now().Add(-time.Second)
}

func keysOf[K comparable, V any](entries []Entry[K, V]) []K {
	keys := make([]K, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, entry.Key)
	}
	return keys
}

func TestLRUAddGetContainsRemove(t *testing.T) {
	cache := New[string, int](0, 0)

	_, found := cache.Get("missing")
	require.False(t, found)
	require.False(t, cache.Contains("missing"))
	require.False(t, cache.Remove("missing"))

	cache.Add("key", 1)
	value, found := cache.Get("key")
	require.True(t, found)
	require.Equal(t, 1, value)
	require.True(t, cache.Contains("key"))

	cache.Add("key", 2)
	value, found = cache.Get("key")
	require.True(t, found)
	require.Equal(t, 2, value)
	require.Equal(t, 1, cache.Len())

	require.True(t, cache.Remove("key"))
	require.False(t, cache.Remove("key"))
	_, found = cache.Get("key")
	require.False(t, found)
	require.False(t, cache.Contains("key"))
	require.Zero(t, cache.Len())
}

func TestLRUDropsTheLeastRecentlyUsedEntry(t *testing.T) {
	testCases := []struct {
		name    string
		touch   func(cache *LRU[string, int])
		dropped string
		kept    []string
	}{
		{
			name:    "without access the oldest entry goes",
			touch:   func(*LRU[string, int]) {},
			dropped: "first",
			kept:    []string{"second", "third"},
		},
		{
			name:    "Get makes an entry the most recently used",
			touch:   func(cache *LRU[string, int]) { cache.Get("first") },
			dropped: "second",
			kept:    []string{"first", "third"},
		},
		{
			name:    "Add of an existing key makes it the most recently used",
			touch:   func(cache *LRU[string, int]) { cache.Add("first", 10) },
			dropped: "second",
			kept:    []string{"first", "third"},
		},
		{
			name:    "Contains does not change the recency",
			touch:   func(cache *LRU[string, int]) { cache.Contains("first") },
			dropped: "first",
			kept:    []string{"second", "third"},
		},
		{
			name:    "Entries does not change the recency",
			touch:   func(cache *LRU[string, int]) { cache.Entries() },
			dropped: "first",
			kept:    []string{"second", "third"},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cache := New[string, int](2, 0)
			cache.Add("first", 1)
			cache.Add("second", 2)
			testCase.touch(cache)
			require.Equal(t, 2, cache.Len())

			cache.Add("third", 3)

			require.Equal(t, 2, cache.Len())
			require.False(t, cache.Contains(testCase.dropped))
			for _, key := range testCase.kept {
				require.True(t, cache.Contains(key), key)
			}
		})
	}
}

func TestLRUSizeZeroOrNegativeIsUnlimited(t *testing.T) {
	for _, size := range []int{0, -1} {
		t.Run("size "+strconv.Itoa(size), func(t *testing.T) {
			cache := New[int, int](size, 0)
			for key := range 1000 {
				cache.Add(key, key)
			}
			require.Equal(t, 1000, cache.Len())
			value, found := cache.Get(0)
			require.True(t, found)
			require.Zero(t, value)
		})
	}
}

func TestLRUTTLZeroOrNegativeNeverExpires(t *testing.T) {
	for _, ttl := range []time.Duration{0, -time.Millisecond} {
		t.Run("ttl "+ttl.String(), func(t *testing.T) {
			cache := New[string, int](0, ttl)
			cache.Add("key", 1)
			require.True(t, cache.elements["key"].Value.(*element[string, int]).expiresAt.IsZero())
			_, found := cache.Get("key")
			require.True(t, found)
		})
	}
}

func TestLRUEntriesExpireAfterTTL(t *testing.T) {
	cache := New[string, int](0, 5*time.Millisecond)
	cache.Add("key", 1)
	_, found := cache.Get("key")
	require.True(t, found)

	require.Eventually(t, func() bool {
		_, found := cache.Get("key")
		return !found
	}, time.Second, time.Millisecond)

	require.False(t, cache.Contains("key"))
	require.Empty(t, cache.Entries())
	require.Equal(t, 1, cache.Len(), "an expired entry is reclaimed by a later write, not by reads")
	require.False(t, cache.Remove("key"), "removing an expired entry reports no live value")
	require.Zero(t, cache.Len())
}

func TestLRUAddRestartsTheTTL(t *testing.T) {
	cache := New[string, int](0, time.Hour)
	cache.Add("key", 1)
	expire(t, cache, "key")
	_, found := cache.Get("key")
	require.False(t, found)

	cache.Add("key", 2)

	value, found := cache.Get("key")
	require.True(t, found)
	require.Equal(t, 2, value)
}

func TestLRUAddSweepsExpiredEntriesAtMostOncePerTTL(t *testing.T) {
	cache := New[string, int](0, time.Hour)
	cache.Add("first", 1) // the first write sweeps and schedules the next sweep an hour later
	expire(t, cache, "first")

	cache.Add("second", 2)
	require.Equal(t, 2, cache.Len(), "no sweep before the ttl has passed")

	cache.nextSweepAt = time.Time{}
	cache.Add("third", 3)
	require.Equal(t, 2, cache.Len())
	require.Equal(t, []string{"second", "third"}, keysOf(cache.Entries()))
}

func TestLRUAddEventuallySweepsUnreadEntries(t *testing.T) {
	cache := New[string, int](0, 5*time.Millisecond)
	cache.Add("first", 1)
	cache.Add("second", 2)

	require.Eventually(t, func() bool {
		cache.Add("third", 3)
		return cache.Len() == 1
	}, time.Second, time.Millisecond)
	require.True(t, cache.Contains("third"))
}

func TestLRUEntriesSkipExpiredEntriesInRecencyOrder(t *testing.T) {
	cache := New[string, int](0, time.Hour)
	cache.Add("first", 1)
	cache.Add("second", 2)
	cache.Add("third", 3)
	cache.Get("first")
	expire(t, cache, "second")

	require.Equal(t, []Entry[string, int]{{Key: "third", Value: 3}, {Key: "first", Value: 1}}, cache.Entries())
}

func TestLRURemoveFuncDeletesMatchingKeys(t *testing.T) {
	cache := New[string, int](0, 0)
	cache.Add("user:1", 1)
	cache.Add("group:1", 2)
	cache.Add("user:2", 3)

	cache.RemoveFunc(func(key string) bool { return strings.HasPrefix(key, "user:") })

	require.Equal(t, []string{"group:1"}, keysOf(cache.Entries()))
}

func TestLRUClearDeletesEveryEntry(t *testing.T) {
	cache := New[string, int](2, time.Hour)
	cache.Add("first", 1)
	cache.Add("second", 2)
	expire(t, cache, "first")

	cache.Clear()

	require.Zero(t, cache.Len())
	require.Empty(t, cache.Entries())
	require.False(t, cache.Contains("second"))

	cache.Add("third", 3)
	cache.Add("fourth", 4)
	cache.Add("fifth", 5)
	require.Equal(t, []string{"fourth", "fifth"}, keysOf(cache.Entries()), "the size bound still holds after Clear")
}

func TestLRUConcurrentAccess(t *testing.T) {
	const (
		size       = 128
		goroutines = 8
		operations = 2000
	)
	cache := New[int, int](size, 50*time.Millisecond)

	var waitGroup sync.WaitGroup
	for goroutine := range goroutines {
		waitGroup.Go(func() {
			for operation := range operations {
				key := (goroutine*operations + operation) % (2 * size)
				cache.Add(key, operation)
				cache.Get(key)
				cache.Contains(key)
				cache.Entries()
				if operation%7 == 0 {
					cache.Remove(key)
				}
				if operation%100 == 0 {
					cache.RemoveFunc(func(candidate int) bool { return candidate == key })
				}
			}
		})
	}
	waitGroup.Wait()

	require.LessOrEqual(t, cache.Len(), size)
}
