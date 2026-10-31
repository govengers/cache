package cache_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/govengers/cache"
	"github.com/stretchr/testify/require"
)

// blockingSet makes the fake's Set signal started and wait for release; it returns what the
// write's context reports when released.
func blockingSet(backend *fakeBackend) (started <-chan struct{}, release func()) {
	startedChannel := make(chan struct{}, 1)
	releaseChannel := make(chan struct{})
	backend.set = func(ctx context.Context, _, _ string) error {
		select {
		case startedChannel <- struct{}{}:
		default:
		}
		<-releaseChannel
		return ctx.Err()
	}
	var releaseOnce sync.Once
	return startedChannel, func() { releaseOnce.Do(func() { close(releaseChannel) }) }
}

func newAsync(t *testing.T, backend *fakeBackend, queueSize int, logs *bytes.Buffer) *cache.Synchronized[string, string] {
	t.Helper()
	options := &cache.SynchronizedOptions[string, string]{Backend: backend, AsyncWrites: true, AsyncQueueSize: queueSize}
	if logs != nil {
		options.Logger = slog.New(slog.NewTextHandler(logs, nil))
	}
	return newSynchronized(t, options)
}

func TestSynchronizedAsyncSetChangesLocallyAndWritesInTheBackground(t *testing.T) {
	backend := newFakeBackend()
	started, release := blockingSet(backend)
	synchronized := newAsync(t, backend, 0, nil)
	t.Cleanup(release)

	ctx, cancel := context.WithCancel(t.Context())
	require.NoError(t, synchronized.Set(ctx, "key", "value"))
	cancel()
	<-started
	requireLocal(t, synchronized, "key", "value")

	release()
	require.NoError(t, synchronized.Close(t.Context()))
	requireLocal(t, synchronized, "key", "value") // the write ignored the caller's cancellation
}

func TestSynchronizedCloseReturnsWhenItsContextEndsWhileAWriteHangs(t *testing.T) {
	backend := newFakeBackend()
	started, release := blockingSet(backend)
	synchronized := newAsync(t, backend, 0, nil)
	t.Cleanup(release)
	require.NoError(t, synchronized.Set(t.Context(), "key", "value"))
	<-started

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, synchronized.Close(ctx), context.DeadlineExceeded)

	release()
	require.NoError(t, synchronized.Close(t.Context()))
}

func TestSynchronizedAsyncWritesAfterCloseReturnErrClosed(t *testing.T) {
	backend := newFakeBackend()
	synchronized := newAsync(t, backend, 0, nil)
	require.NoError(t, synchronized.Close(t.Context()))

	require.ErrorIs(t, synchronized.Set(t.Context(), "key", "value"), cache.ErrClosed)
	require.ErrorIs(t, synchronized.Remove(t.Context(), "key"), cache.ErrClosed)
	require.ErrorIs(t, synchronized.RemovePrefix(t.Context(), "key"), cache.ErrClosed)
}

func TestSynchronizedAsyncSetThatIsNotQueuedLeavesNoLocalCopy(t *testing.T) {
	t.Run("after Close", func(t *testing.T) {
		backend := newFakeBackend()
		synchronized := newAsync(t, backend, 0, nil)
		require.NoError(t, synchronized.Close(t.Context()))

		require.ErrorIs(t, synchronized.Set(t.Context(), "key", "value"), cache.ErrClosed)

		requireNotLocal(t, synchronized, "key")
	})
	t.Run("queue full", func(t *testing.T) {
		backend := newFakeBackend()
		started, release := blockingSet(backend)
		synchronized := newAsync(t, backend, 1, nil)
		t.Cleanup(release)
		require.NoError(t, synchronized.Set(t.Context(), "running", "value"))
		<-started
		require.NoError(t, synchronized.Set(t.Context(), "queued", "value"))

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, synchronized.Set(ctx, "rejected", "value"), context.Canceled)

		requireNotLocal(t, synchronized, "rejected")
	})
}

func TestSynchronizedAsyncWriteWaitsForRoomInTheQueue(t *testing.T) {
	backend := newFakeBackend()
	started, release := blockingSet(backend)
	synchronized := newAsync(t, backend, 1, nil)
	t.Cleanup(release)
	require.NoError(t, synchronized.Set(t.Context(), "running", "value"))
	<-started
	require.NoError(t, synchronized.Set(t.Context(), "queued", "value"))

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	err := synchronized.Set(ctx, "waiting", "value")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "cache: queue background set")

	queued := make(chan error, 1)
	go func() { queued <- synchronized.Set(context.Background(), "waiting", "value") }()
	release()
	require.NoError(t, <-queued, "the write is queued once there is room")
	require.NoError(t, synchronized.Close(t.Context()))
}

// TestSynchronizedCloseHonorsItsContextWhileAWriteWaitsForRoom proves that Close(ctx) returns at
// its deadline although another goroutine's write waits for room in a full queue.
func TestSynchronizedCloseHonorsItsContextWhileAWriteWaitsForRoom(t *testing.T) {
	backend := newFakeBackend()
	started, release := blockingSet(backend)
	synchronized := newAsync(t, backend, 1, nil)
	t.Cleanup(release)
	require.NoError(t, synchronized.Set(t.Context(), "running", "value"))
	<-started
	require.NoError(t, synchronized.Set(t.Context(), "queued", "value"))
	waiting := make(chan error, 1)
	go func() { waiting <- synchronized.Set(context.Background(), "waiting", "value") }()
	require.Eventually(t, func() bool { return goroutineBlockedIn("(*backgroundWriter).enqueue") }, time.Second, time.Millisecond)

	closed := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		closed <- synchronized.Close(ctx)
	}()

	select {
	case err := <-closed:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		release()
		require.Fail(t, "Close ignored its deadline while a write waited for room in the queue", "Close returned %v only after the hanging write was released", <-closed)
	}
	release()
	waitingError := <-waiting
	require.True(t, waitingError == nil || errors.Is(waitingError, cache.ErrClosed), "unexpected error %v", waitingError)
}

func goroutineBlockedIn(function string) bool {
	stacks := make([]byte, 1<<20)
	return strings.Contains(string(stacks[:runtime.Stack(stacks, true)]), function)
}

func TestSynchronizedAsyncWritesReachTheBackendInOrder(t *testing.T) {
	var operations []string // written by the background goroutine only, read after Close
	backend := newFakeBackend()
	backend.set = func(_ context.Context, key, value string) error {
		operations = append(operations, "set "+key+"="+value)
		return nil
	}
	backend.remove = func(_ context.Context, key string) error {
		operations = append(operations, "remove "+key)
		return nil
	}
	backend.removePrefix = func(_ context.Context, prefix string) error {
		operations = append(operations, "remove prefix "+prefix)
		return nil
	}
	synchronized := newAsync(t, backend, 2, nil)

	var expected []string
	for round := range 50 {
		value := fmt.Sprint(round)
		require.NoError(t, synchronized.Set(t.Context(), "key", value))
		require.NoError(t, synchronized.Remove(t.Context(), "key"))
		require.NoError(t, synchronized.RemovePrefix(t.Context(), "k"))
		expected = append(expected, "set key="+value, "remove key", "remove prefix k")
	}
	require.NoError(t, synchronized.Close(t.Context()))

	require.Equal(t, expected, operations)
}

func TestSynchronizedAsyncTimeoutIsTheDeadlineOfABackgroundWrite(t *testing.T) {
	testCases := []struct {
		name         string
		asyncTimeout time.Duration
		expected     time.Duration
	}{
		{name: "default", asyncTimeout: 0, expected: 10 * time.Second},
		{name: "configured", asyncTimeout: time.Minute, expected: time.Minute},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			deadlines := make(chan time.Duration, 1)
			backend := newFakeBackend()
			backend.set = func(ctx context.Context, _, _ string) error {
				deadline, found := ctx.Deadline()
				if !found {
					deadline = time.Now() // fails the range check below
				}
				deadlines <- time.Until(deadline)
				return ctx.Err()
			}
			synchronized := newSynchronized(t, &cache.SynchronizedOptions[string, string]{
				Backend:      backend,
				AsyncWrites:  true,
				AsyncTimeout: testCase.asyncTimeout,
			})

			ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
			defer cancel()
			require.NoError(t, synchronized.Set(ctx, "key", "value"))

			remaining := <-deadlines
			require.LessOrEqual(t, remaining, testCase.expected)
			require.Greater(t, remaining, testCase.expected-time.Second, "the caller's 1ms deadline was replaced")
			require.NoError(t, synchronized.Close(t.Context()))
			requireLocal(t, synchronized, "key", "value")
		})
	}
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
			backend := newFakeBackend()
			synchronized := newAsync(t, backend, 0, &logs)
			backend.emit(cache.Event[string, string]{Type: cache.EventSet, Key: "key", Value: "old"})

			require.NoError(t, testCase.write(t.Context(), synchronized))
			require.NoError(t, synchronized.Close(t.Context()))

			requireNotLocal(t, synchronized, "key")
			require.Contains(t, logs.String(), testCase.message)
			require.Contains(t, logs.String(), errBackend.Error())
		})
	}
}

func TestSynchronizedAsyncWritePanicIsRecoveredAndLogged(t *testing.T) {
	var logs bytes.Buffer
	backend := newFakeBackend()
	backend.set = func(context.Context, string, string) error {
		panic("backend exploded")
	}
	synchronized := newAsync(t, backend, 0, &logs)

	require.NoError(t, synchronized.Set(t.Context(), "key", "value"))
	require.NoError(t, synchronized.Close(t.Context()))

	require.Contains(t, logs.String(), "cache: background set panicked")
	require.Contains(t, logs.String(), "backend exploded")
	requireNotLocal(t, synchronized, "key")
}
