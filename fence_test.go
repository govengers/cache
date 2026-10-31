package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadFenceKeepsAReadUnlessItsKeyOrAPrefixChanged(t *testing.T) {
	testCases := []struct {
		name   string
		change func(fence *readFence[string, string])
		kept   bool
	}{
		{name: "no change", change: func(*readFence[string, string]) {}, kept: true},
		{name: "another key", change: func(fence *readFence[string, string]) { fence.changeKey("user:2", func() {}) }, kept: true},
		{name: "the key", change: func(fence *readFence[string, string]) { fence.changeKey("user:1", func() {}) }},
		{name: "a prefix covering the key", change: func(fence *readFence[string, string]) { fence.changePrefix("user:", func() {}) }},
		{name: "a prefix not covering the key", change: func(fence *readFence[string, string]) { fence.changePrefix("group:", func() {}) }},
		{name: "a reset", change: func(fence *readFence[string, string]) { fence.changePrefix("", func() {}) }},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fence := newReadFence[string, string](StringKeyCodec{})
			ticket := fence.beginRead("user:1")
			testCase.change(fence)

			kept := false
			fence.finishRead("user:1", ticket, func() { kept = true })

			require.Equal(t, testCase.kept, kept)
			require.Empty(t, fence.readsInFlight)
		})
	}
}

func TestReadFenceTracksConcurrentReadsOfOneKey(t *testing.T) {
	fence := newReadFence[string, string](StringKeyCodec{})
	kept := map[string]bool{}
	keep := func(read string) func() { return func() { kept[read] = true } }

	beforeChange := fence.beginRead("key")
	alsoBeforeChange := fence.beginRead("key")
	fence.changeKey("key", func() {})
	afterChange := fence.beginRead("key")

	fence.finishRead("key", beforeChange, keep("before change"))
	require.Contains(t, fence.readsInFlight, "key", "the other reads are still in flight")
	fence.finishRead("key", afterChange, keep("after change"))
	fence.finishRead("key", alsoBeforeChange, keep("also before change"))

	require.Equal(t, map[string]bool{"after change": true}, kept)
	require.Empty(t, fence.readsInFlight)
}

func TestReadFenceDoesNotTrackKeysWithoutReads(t *testing.T) {
	fence := newReadFence[string, string](StringKeyCodec{})
	applied := 0

	fence.changeKey("key", func() { applied++ })
	fence.changePrefix("k", func() { applied++ })

	require.Equal(t, 2, applied)
	require.Empty(t, fence.readsInFlight)
	ticket := fence.beginRead("key")
	kept := false
	fence.finishRead("key", ticket, func() { kept = true })
	require.True(t, kept, "changes before a read do not drop it")
}

func TestReadFenceLoadSkipsWhatChangedWhileItRan(t *testing.T) {
	entries := []Entry[int, string]{{Key: 1, Value: "one"}, {Key: 12, Value: "twelve"}, {Key: 2, Value: "two"}}
	testCases := []struct {
		name   string
		change func(fence *readFence[int, string])
		kept   []int
	}{
		{name: "no change", change: func(*readFence[int, string]) {}, kept: []int{1, 12, 2}},
		{name: "a key", change: func(fence *readFence[int, string]) { fence.changeKey(12, func() {}) }, kept: []int{1, 2}},
		{name: "a key that was not loaded", change: func(fence *readFence[int, string]) { fence.changeKey(3, func() {}) }, kept: []int{1, 12, 2}},
		{name: "a prefix on the encoded key", change: func(fence *readFence[int, string]) { fence.changePrefix("1", func() {}) }, kept: []int{2}},
		{name: "a prefix matching nothing", change: func(fence *readFence[int, string]) { fence.changePrefix("3", func() {}) }, kept: []int{1, 12, 2}},
		{name: "a reset", change: func(fence *readFence[int, string]) { fence.changePrefix("", func() {}) }, kept: nil},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fence := newReadFence[int, string](IntKeyCodec{})
			tracker := fence.beginLoad()
			testCase.change(fence)

			var kept []int
			fence.finishLoad(tracker, entries, func(key int, _ string) { kept = append(kept, key) })

			require.Equal(t, testCase.kept, kept)
			require.Empty(t, fence.loadsInFlight)
		})
	}
}

func TestReadFenceLoadsAreTrackedSeparately(t *testing.T) {
	fence := newReadFence[string, string](StringKeyCodec{})
	entries := []Entry[string, string]{{Key: "first", Value: "one"}, {Key: "second", Value: "two"}}

	earlier := fence.beginLoad()
	fence.changeKey("first", func() {})
	later := fence.beginLoad()
	fence.changeKey("second", func() {})

	var keptByEarlier, keptByLater []string
	fence.finishLoad(earlier, entries, func(key, _ string) { keptByEarlier = append(keptByEarlier, key) })
	fence.finishLoad(later, entries, func(key, _ string) { keptByLater = append(keptByLater, key) })

	require.Empty(t, keptByEarlier)
	require.Equal(t, []string{"first"}, keptByLater)
	require.Empty(t, fence.loadsInFlight)
}

func TestReadFenceFailedLoadStopsTracking(t *testing.T) {
	fence := newReadFence[string, string](StringKeyCodec{})
	tracker := fence.beginLoad()

	fence.finishLoad(tracker, nil, nil)

	require.Empty(t, fence.loadsInFlight)
}
