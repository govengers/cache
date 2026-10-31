package cache_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
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

// fakeBackend is a Backend whose Get, Set and Load run the functions a test sets. Every other
// method, and an unset function, fails with errBackend, so a successful read proves a local hit.
type fakeBackend struct {
	get     func(ctx context.Context, key string) (string, error)
	set     func(ctx context.Context, key, value string) error
	load    func(ctx context.Context) ([]cache.Entry[string, string], error)
	handler func(cache.Event[string, string])
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

func (b *fakeBackend) Remove(context.Context, string) error {
	return errBackend
}

func (b *fakeBackend) RemovePrefix(context.Context, string) error {
	return errBackend
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

func (b *fakeBackend) OnEvent(handler func(cache.Event[string, string])) {
	b.handler = handler
}

func (b *fakeBackend) emit(event cache.Event[string, string]) {
	b.handler(event)
}

func newSynchronized(t *testing.T, options *cache.SynchronizedOptions[string, string]) *cache.Synchronized[string, string] {
	t.Helper()
	if options.KeyCodec == nil {
		options.KeyCodec = cache.StringKeyCodec{}
	}
	synchronized, err := cache.NewSynchronized(options)
	require.NoError(t, err)
	t.Cleanup(synchronized.Close)
	return synchronized
}

// requireLocal fails unless key holds value locally; the fake backend fails every Get.
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
		{name: "missing Backend", options: &cache.SynchronizedOptions[string, string]{KeyCodec: cache.StringKeyCodec{}}},
		{name: "missing KeyCodec", options: &cache.SynchronizedOptions[string, string]{Backend: &fakeBackend{}}},
		{name: "negative LocalTTL", options: &cache.SynchronizedOptions[string, string]{Backend: &fakeBackend{}, KeyCodec: cache.StringKeyCodec{}, LocalTTL: -time.Second}},
		{name: "negative LocalSize", options: &cache.SynchronizedOptions[string, string]{Backend: &fakeBackend{}, KeyCodec: cache.StringKeyCodec{}, LocalSize: -1}},
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
			synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: &fakeBackend{}})
			require.ErrorIs(t, testCase.operation(t.Context(), synchronized), errBackend)
		})
	}
}

func TestSynchronizedFailedWriteKeepsTheLocalCache(t *testing.T) {
	backend := &fakeBackend{}
	synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: backend})
	backend.emit(cache.Event[string, string]{Type: cache.EventSet, Key: "kept", Value: "value"})

	require.ErrorIs(t, synchronized.Set(t.Context(), "key", "value"), errBackend)
	require.ErrorIs(t, synchronized.Remove(t.Context(), "kept"), errBackend)
	require.ErrorIs(t, synchronized.RemovePrefix(t.Context(), ""), errBackend)

	requireNotLocal(t, synchronized, "key")
	requireLocal(t, synchronized, "kept", "value")
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
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			backend := &fakeBackend{}
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

func TestSynchronizedGetDropsAReadOvertakenByAChange(t *testing.T) {
	getStarted := make(chan struct{})
	releaseGet := make(chan struct{})
	backend := &fakeBackend{get: func(context.Context, string) (string, error) {
		close(getStarted)
		<-releaseGet
		return "stale", nil
	}}
	synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: backend})

	read := make(chan string)
	go func() {
		value, _ := synchronized.Get(context.Background(), "key")
		read <- value
	}()
	<-getStarted
	backend.emit(cache.Event[string, string]{Type: cache.EventSet, Key: "key", Value: "fresh"})
	close(releaseGet)

	require.Equal(t, "stale", <-read, "the caller still gets what it read")
	requireLocal(t, synchronized, "key", "fresh")
}

func TestSynchronizedPreloadFillsTheLocalCache(t *testing.T) {
	backend := &fakeBackend{load: func(context.Context) ([]cache.Entry[string, string], error) {
		return []cache.Entry[string, string]{{Key: "first", Value: "one"}, {Key: "second", Value: "two"}}, nil
	}}
	synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: backend})

	require.NoError(t, synchronized.Preload(t.Context()))

	requireLocal(t, synchronized, "first", "one")
	requireLocal(t, synchronized, "second", "two")
}

func TestSynchronizedPreloadDropsEntriesOvertakenByAChange(t *testing.T) {
	backend := &fakeBackend{}
	backend.load = func(context.Context) ([]cache.Entry[string, string], error) {
		backend.emit(cache.Event[string, string]{Type: cache.EventSet, Key: "changed", Value: "fresh"})
		return []cache.Entry[string, string]{{Key: "changed", Value: "stale"}, {Key: "other", Value: "value"}}, nil
	}
	synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: backend})

	require.NoError(t, synchronized.Preload(t.Context()))

	requireLocal(t, synchronized, "changed", "fresh")
	requireNotLocal(t, synchronized, "other")
}

func TestSynchronizedAsyncSetChangesLocallyAndWritesInTheBackground(t *testing.T) {
	setStarted := make(chan struct{})
	releaseSet := make(chan struct{})
	backend := &fakeBackend{set: func(ctx context.Context, _, _ string) error {
		close(setStarted)
		<-releaseSet
		return ctx.Err()
	}}
	synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{Backend: backend, AsyncWrites: true})

	ctx, cancel := context.WithCancel(t.Context())
	require.NoError(t, synchronized.Set(ctx, "key", "value"))
	cancel()
	<-setStarted
	requireLocal(t, synchronized, "key", "value")

	closed := make(chan struct{})
	go func() {
		synchronized.Close()
		close(closed)
	}()
	require.Never(t, func() bool {
		select {
		case <-closed:
			return true
		default:
			return false
		}
	}, 20*time.Millisecond, time.Millisecond, "Close waits for the background write")

	close(releaseSet)
	<-closed
	requireLocal(t, synchronized, "key", "value") // the write ignored the caller's cancellation
}

func TestSynchronizedAsyncWriteFailureIsLogged(t *testing.T) {
	testCases := []struct {
		name    string
		write   func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error
		message string
	}{
		{
			name: "set drops the local copy",
			write: func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error {
				return synchronized.Set(ctx, "key", "new")
			},
			message: "cache: background set failed",
		},
		{
			name: "remove",
			write: func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error {
				return synchronized.Remove(ctx, "key")
			},
			message: "cache: background remove failed",
		},
		{
			name: "remove prefix",
			write: func(ctx context.Context, synchronized *cache.Synchronized[string, string]) error {
				return synchronized.RemovePrefix(ctx, "k")
			},
			message: "cache: background remove prefix failed",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var logs bytes.Buffer
			backend := &fakeBackend{}
			synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{
				Backend:     backend,
				AsyncWrites: true,
				Logger:      slog.New(slog.NewTextHandler(&logs, nil)),
			})
			backend.emit(cache.Event[string, string]{Type: cache.EventSet, Key: "key", Value: "old"})

			require.NoError(t, testCase.write(t.Context(), synchronized))
			synchronized.Close()

			requireNotLocal(t, synchronized, "key")
			require.Contains(t, logs.String(), testCase.message)
			require.Contains(t, logs.String(), errBackend.Error())
		})
	}
}

func TestSynchronizedAsyncWritePanicIsRecoveredAndLogged(t *testing.T) {
	var logs bytes.Buffer
	backend := &fakeBackend{set: func(context.Context, string, string) error {
		panic("backend exploded")
	}}
	synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{
		Backend:     backend,
		AsyncWrites: true,
		Logger:      slog.New(slog.NewTextHandler(&logs, nil)),
	})

	require.NoError(t, synchronized.Set(t.Context(), "key", "value"))
	synchronized.Close()

	require.Contains(t, logs.String(), "cache: background set panicked")
	require.Contains(t, logs.String(), "backend exploded")
	requireNotLocal(t, synchronized, "key")
}
