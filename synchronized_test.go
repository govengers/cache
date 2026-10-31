package cache_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/govengers/cache"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

var errBackend = errors.New("backend failed")

// fakeBackend is a Backend whose methods run the functions a test sets. TTL, Contains and an
// unset function fail with errBackend, so a successful Get proves a local hit.
type fakeBackend struct {
	keyCodec     cache.KeyCodec[string]
	get          func(ctx context.Context, key string) (string, error)
	set          func(ctx context.Context, key, value string) error
	remove       func(ctx context.Context, key string) error
	removePrefix func(ctx context.Context, prefix string) error
	load         func(ctx context.Context) ([]cache.Entry[string, string], error)
	handler      func(cache.Event[string, string])
	unregistered int
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{keyCodec: cache.StringKeyCodec{}}
}

func (b *fakeBackend) Get(ctx context.Context, key string) (string, error) {
	if b.get == nil {
		return "", errBackend
	}
	return b.get(ctx, key)
}

func (b *fakeBackend) TTL(context.Context, string) (time.Duration, error) {
	return 0, errBackend
}

func (b *fakeBackend) Set(ctx context.Context, key, value string) error {
	if b.set == nil {
		return errBackend
	}
	return b.set(ctx, key, value)
}

func (b *fakeBackend) Remove(ctx context.Context, key string) error {
	if b.remove == nil {
		return errBackend
	}
	return b.remove(ctx, key)
}

func (b *fakeBackend) RemovePrefix(ctx context.Context, prefix string) error {
	if b.removePrefix == nil {
		return errBackend
	}
	return b.removePrefix(ctx, prefix)
}

func (b *fakeBackend) Contains(context.Context, string) (bool, error) {
	return false, errBackend
}

func (b *fakeBackend) Load(ctx context.Context) ([]cache.Entry[string, string], error) {
	if b.load == nil {
		return nil, errBackend
	}
	return b.load(ctx)
}

func (b *fakeBackend) KeyCodec() cache.KeyCodec[string] {
	return b.keyCodec
}

func (b *fakeBackend) OnEvent(handler func(cache.Event[string, string])) func() {
	b.handler = handler
	return func() { b.unregistered++ }
}

func (b *fakeBackend) emit(event cache.Event[string, string]) {
	b.handler(event)
}

func newSynchronized(t *testing.T, options *cache.SynchronizedOptions[string, string]) *cache.Synchronized[string, string] {
	t.Helper()
	synchronized, err := cache.NewSynchronized(options)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, synchronized.Close(context.Background())) })
	return synchronized
}

// requireLocal fails unless key holds value locally; the fake backend fails every Get it has no
// function for.
func requireLocal(t *testing.T, synchronized *cache.Synchronized[string, string], key, value string) {
	t.Helper()
	stored, err := synchronized.Get(t.Context(), key)
	require.NoError(t, err, key)
	require.Equal(t, value, stored, key)
}

func requireNotLocal(t *testing.T, synchronized *cache.Synchronized[string, string], key string) {
	t.Helper()
	_, err := synchronized.Get(t.Context(), key)
	require.ErrorIs(t, err, errBackend, key)
}

func TestNewSynchronizedRejectsInvalidOptions(t *testing.T) {
	testCases := []struct {
		name    string
		options *cache.SynchronizedOptions[string, string]
	}{
		{name: "nil options", options: nil},
		{name: "missing Backend", options: &cache.SynchronizedOptions[string, string]{}},
		{name: "Backend without KeyCodec", options: &cache.SynchronizedOptions[string, string]{Backend: &fakeBackend{}}},
		{name: "negative LocalTTL", options: &cache.SynchronizedOptions[string, string]{Backend: newFakeBackend(), LocalTTL: -time.Second}},
		{name: "negative LocalSize", options: &cache.SynchronizedOptions[string, string]{Backend: newFakeBackend(), LocalSize: -1}},
		{name: "negative AsyncQueueSize", options: &cache.SynchronizedOptions[string, string]{Backend: newFakeBackend(), AsyncQueueSize: -1}},
		{name: "negative AsyncTimeout", options: &cache.SynchronizedOptions[string, string]{Backend: newFakeBackend(), AsyncTimeout: -time.Second}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			synchronized, err := cache.NewSynchronized(testCase.options)
			require.Error(t, err)
			require.Nil(t, synchronized)
		})
	}
}

func TestSynchronizedReturnsBackendErrors(t *testing.T) {
	testCases := []struct {
		name      string
		operation func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error
	}{
		{name: "Get", operation: func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error {
			_, err := synchronized.Get(ctx, "key")
			return err
		}},
		{name: "TTL", operation: func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error {
			_, err := synchronized.TTL(ctx, "key")
			return err
		}},
		{name: "Set", operation: func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error {
			return synchronized.Set(ctx, "key", "value")
		}},
		{name: "Remove", operation: func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error {
			return synchronized.Remove(ctx, "key")
		}},
		{name: "RemovePrefix", operation: func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error {
			return synchronized.RemovePrefix(ctx, "key")
		}},
		{name: "Contains", operation: func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error {
			_, err := synchronized.Contains(ctx, "key")
			return err
		}},
		{name: "Preload", operation: func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error {
			return synchronized.Preload(ctx)
		}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: newFakeBackend()})
			require.ErrorIs(t, testCase.operation(t.Context(), synchronized), errBackend)
		})
	}
}

func TestSynchronizedFailedWriteKeepsTheLocalCache(t *testing.T) {
	backend := newFakeBackend()
	synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: backend})
	backend.emit(cache.Event[string, string]{Type: cache.EventSet, Key: "kept", Value: "value"})

	require.ErrorIs(t, synchronized.Set(t.Context(), "key", "value"), errBackend)
	require.ErrorIs(t, synchronized.Remove(t.Context(), "kept"), errBackend)
	require.ErrorIs(t, synchronized.RemovePrefix(t.Context(), ""), errBackend)

	requireNotLocal(t, synchronized, "key")
	requireLocal(t, synchronized, "kept", "value")
}

func TestSynchronizedWritesChangeTheLocalCache(t *testing.T) {
	backend := newFakeBackend()
	backend.set = func(context.Context, string, string) error { return nil }
	backend.remove = func(context.Context, string) error { return nil }
	backend.removePrefix = func(context.Context, string) error { return nil }
	synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: backend})
	ctx := t.Context()
	for _, key := range []string{"user:1", "user:2", "user:3", "group:1"} {
		require.NoError(t, synchronized.Set(ctx, key, key+" value"))
	}

	require.NoError(t, synchronized.Remove(ctx, "user:1"))
	require.NoError(t, synchronized.RemovePrefix(ctx, "user:"))

	requireLocal(t, synchronized, "group:1", "group:1 value")
	for _, key := range []string{"user:1", "user:2", "user:3"} {
		requireNotLocal(t, synchronized, key)
	}
}

func TestSynchronizedAppliesBackendEvents(t *testing.T) {
	testCases := []struct {
		name      string
		event     cache.Event[string, string]
		remaining map[string]string
	}{
		{
			name:      "set",
			event:     cache.Event[string, string]{Type: cache.EventSet, Key: "user:1", Value: "changed"},
			remaining: map[string]string{"user:1": "changed", "user:2": "two", "group:1": "group"},
		},
		{
			name:      "remove",
			event:     cache.Event[string, string]{Type: cache.EventRemove, Key: "user:1"},
			remaining: map[string]string{"user:2": "two", "group:1": "group"},
		},
		{
			name:      "remove prefix",
			event:     cache.Event[string, string]{Type: cache.EventRemovePrefix, Prefix: "user:"},
			remaining: map[string]string{"group:1": "group"},
		},
		{
			name:      "reset clears the local cache",
			event:     cache.Event[string, string]{Type: cache.EventReset},
			remaining: map[string]string{},
		},
		{
			name:      "unknown type is ignored",
			event:     cache.Event[string, string]{Type: cache.EventType(99), Key: "user:1"},
			remaining: map[string]string{"user:1": "one", "user:2": "two", "group:1": "group"},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			backend := newFakeBackend()
			synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: backend})
			initial := map[string]string{"user:1": "one", "user:2": "two", "group:1": "group"}
			for key, value := range initial {
				backend.emit(cache.Event[string, string]{Type: cache.EventSet, Key: key, Value: value})
			}

			backend.emit(testCase.event)

			for key := range initial {
				if value, kept := testCase.remaining[key]; kept {
					requireLocal(t, synchronized, key, value)
				} else {
					requireNotLocal(t, synchronized, key)
				}
			}
		})
	}
}

func TestEventTypeString(t *testing.T) {
	testCases := []struct {
		eventType cache.EventType
		name      string
	}{
		{eventType: cache.EventSet, name: "set"},
		{eventType: cache.EventRemove, name: "remove"},
		{eventType: cache.EventRemovePrefix, name: "remove prefix"},
		{eventType: cache.EventReset, name: "reset"},
		{eventType: cache.EventType(0), name: "EventType(0)"},
		{eventType: cache.EventType(42), name: "EventType(42)"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.name, testCase.eventType.String())
		})
	}
}

func TestSynchronizedCloseUnregistersFromTheBackendOnce(t *testing.T) {
	backend := newFakeBackend()
	synchronized, err := cache.NewSynchronized(&cache.SynchronizedOptions[string, string]{Backend: backend})
	require.NoError(t, err)

	require.NoError(t, synchronized.Close(t.Context()))
	require.NoError(t, synchronized.Close(t.Context()))

	require.Equal(t, 1, backend.unregistered)
}

// changesDuringRead are changes a test applies while a read from the backend is in flight, and
// whether the read value is still kept afterwards. The read is of "user:1".
var changesDuringRead = []struct {
	name   string
	events []cache.Event[string, string]
	kept   bool
}{
	{name: "no change", kept: true},
	{
		name:   "a change of another key",
		events: []cache.Event[string, string]{{Type: cache.EventSet, Key: "user:2", Value: "other"}, {Type: cache.EventRemove, Key: "group:1"}},
		kept:   true,
	},
	{name: "a set of the key", events: []cache.Event[string, string]{{Type: cache.EventSet, Key: "user:1", Value: "fresh"}}},
	{name: "a removal of the key", events: []cache.Event[string, string]{{Type: cache.EventRemove, Key: "user:1"}}},
	{name: "a prefix removal covering the key", events: []cache.Event[string, string]{{Type: cache.EventRemovePrefix, Prefix: "user:"}}},
	{name: "a prefix removal not covering the key", events: []cache.Event[string, string]{{Type: cache.EventRemovePrefix, Prefix: "group:"}}, kept: true},
	{name: "a reset", events: []cache.Event[string, string]{{Type: cache.EventReset}}},
}

// localAfterChanges is what the local cache holds for "user:1" after the changes alone.
func localAfterChanges(events []cache.Event[string, string]) (string, bool) {
	for _, event := range events {
		if event.Type == cache.EventSet && event.Key == "user:1" {
			return event.Value, true
		}
	}
	return "", false
}

func requireReadOutcome(t *testing.T, synchronized *cache.Synchronized[string, string], read string, events []cache.Event[string, string], kept bool) {
	t.Helper()
	switch value, changed := localAfterChanges(events); {
	case kept:
		requireLocal(t, synchronized, "user:1", read)
	case changed:
		requireLocal(t, synchronized, "user:1", value)
	default:
		requireNotLocal(t, synchronized, "user:1")
	}
}

func TestSynchronizedGetKeepsAReadUnlessItsKeyChangedMeanwhile(t *testing.T) {
	for _, testCase := range changesDuringRead {
		t.Run(testCase.name, func(t *testing.T) {
			backend := newFakeBackend()
			backend.get = func(context.Context, string) (string, error) {
				for _, event := range testCase.events {
					backend.emit(event)
				}
				backend.get = nil
				return "read", nil
			}
			synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: backend})

			value, err := synchronized.Get(t.Context(), "user:1")
			require.NoError(t, err)
			require.Equal(t, "read", value, "the caller gets what was read either way")

			requireReadOutcome(t, synchronized, "read", testCase.events, testCase.kept)
		})
	}
}

func TestSynchronizedSetKeepsItsValueUnlessTheKeyChangedMeanwhile(t *testing.T) {
	for _, testCase := range changesDuringRead {
		t.Run(testCase.name, func(t *testing.T) {
			backend := newFakeBackend()
			backend.set = func(context.Context, string, string) error {
				for _, event := range testCase.events {
					backend.emit(event)
				}
				return nil
			}
			synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: backend})

			require.NoError(t, synchronized.Set(t.Context(), "user:1", "written"))

			requireReadOutcome(t, synchronized, "written", testCase.events, testCase.kept)
		})
	}
}

func TestSynchronizedPreloadSkipsEntriesChangedMeanwhile(t *testing.T) {
	testCases := []struct {
		name   string
		events []cache.Event[string, string]
		local  map[string]string // the local cache afterwards; a missing key is not held
	}{
		{
			name:  "no change",
			local: map[string]string{"user:1": "one", "user:2": "two", "group:1": "group"},
		},
		{
			name:   "a set of one key",
			events: []cache.Event[string, string]{{Type: cache.EventSet, Key: "user:1", Value: "fresh"}},
			local:  map[string]string{"user:1": "fresh", "user:2": "two", "group:1": "group"},
		},
		{
			name:   "a removal of one key",
			events: []cache.Event[string, string]{{Type: cache.EventRemove, Key: "user:2"}},
			local:  map[string]string{"user:1": "one", "group:1": "group"},
		},
		{
			name:   "a prefix removal",
			events: []cache.Event[string, string]{{Type: cache.EventRemovePrefix, Prefix: "user:"}},
			local:  map[string]string{"group:1": "group"},
		},
		{
			name:   "a reset",
			events: []cache.Event[string, string]{{Type: cache.EventReset}},
			local:  map[string]string{},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			backend := newFakeBackend()
			backend.load = func(context.Context) ([]cache.Entry[string, string], error) {
				for _, event := range testCase.events {
					backend.emit(event)
				}
				return []cache.Entry[string, string]{
					{Key: "user:1", Value: "one"},
					{Key: "user:2", Value: "two"},
					{Key: "group:1", Value: "group"},
				}, nil
			}
			synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: backend})

			require.NoError(t, synchronized.Preload(t.Context()))

			for _, key := range []string{"user:1", "user:2", "group:1"} {
				if value, held := testCase.local[key]; held {
					requireLocal(t, synchronized, key, value)
				} else {
					requireNotLocal(t, synchronized, key)
				}
			}
		})
	}
}

func TestSynchronizedEveryMethodReturnsErrClosedAfterClose(t *testing.T) {
	operations := map[string]func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error{
		"Get": func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error {
			_, err := synchronized.Get(ctx, "key")
			return err
		},
		"TTL": func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error {
			_, err := synchronized.TTL(ctx, "key")
			return err
		},
		"Set": func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error {
			return synchronized.Set(ctx, "key", "value")
		},
		"Remove": func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error {
			return synchronized.Remove(ctx, "key")
		},
		"RemovePrefix": func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error {
			return synchronized.RemovePrefix(ctx, "key")
		},
		"Contains": func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error {
			_, err := synchronized.Contains(ctx, "key")
			return err
		},
		"Preload": func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error {
			return synchronized.Preload(ctx)
		},
	}
	for _, asyncWrites := range []bool{false, true} {
		for name, operation := range operations {
			t.Run(fmt.Sprintf("%s async %t", name, asyncWrites), func(t *testing.T) {
				backend := newFakeBackend()
				synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: backend, AsyncWrites: asyncWrites})
				backend.emit(cache.Event[string, string]{Type: cache.EventSet, Key: "key", Value: "local"}) // a local hit is refused too
				require.NoError(t, synchronized.Close(t.Context()))

				require.ErrorIs(t, operation(t.Context(), synchronized), cache.ErrClosed)
			})
		}
	}
}

func TestSynchronizedGetInFlightAcrossASetKeepsTheSetValue(t *testing.T) {
	var synchronized *cache.Synchronized[string, string]
	backend := newFakeBackend()
	backend.set = func(context.Context, string, string) error { return nil }
	backend.get = func(ctx context.Context, key string) (string, error) {
		backend.get = nil
		require.NoError(t, synchronized.Set(ctx, key, "written"))
		return "read before the write", nil
	}
	synchronized = newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: backend})

	value, err := synchronized.Get(t.Context(), "key")
	require.NoError(t, err)
	require.Equal(t, "read before the write", value)

	requireLocal(t, synchronized, "key", "written")
}

func TestSynchronizedPreloadInFlightAcrossASetSkipsThatKey(t *testing.T) {
	var synchronized *cache.Synchronized[string, string]
	backend := newFakeBackend()
	backend.set = func(context.Context, string, string) error { return nil }
	backend.load = func(ctx context.Context) ([]cache.Entry[string, string], error) {
		require.NoError(t, synchronized.Set(ctx, "written", "new"))
		return []cache.Entry[string, string]{{Key: "written", Value: "old"}, {Key: "other", Value: "value"}}, nil
	}
	synchronized = newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: backend})

	require.NoError(t, synchronized.Preload(t.Context()))

	requireLocal(t, synchronized, "written", "new")
	requireLocal(t, synchronized, "other", "value")
}
