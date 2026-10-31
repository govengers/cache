package cache_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
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
	releaseSet := make(chan struct{})
	writeErrors := make(chan error, 1)
	backend := newFakeBackend()
	backend.set = func(ctx context.Context, _, _ string) error {
		<-releaseSet
		writeErrors <- ctx.Err()
		return ctx.Err()
	}
	synchronized := newAsync(t, backend, 0, nil)

	ctx, cancel := context.WithCancel(t.Context())
	require.NoError(t, synchronized.Set(ctx, "key", "value"))
	cancel()
	requireLocal(t, synchronized, "key", "value")

	close(releaseSet)
	require.NoError(t, <-writeErrors, "the write ignores the caller's cancellation")
	require.NoError(t, synchronized.Close(t.Context()))
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

func TestSynchronizedAsyncWriteWithAnEndedContextIsNeverQueued(t *testing.T) {
	var writes int // counted by the background goroutine, read after Close
	backend := newFakeBackend()
	backend.set = func(context.Context, string, string) error {
		writes++
		return nil
	}
	backend.remove = func(context.Context, string) error {
		writes++
		return nil
	}
	backend.removePrefix = func(context.Context, string) error {
		writes++
		return nil
	}
	synchronized := newAsync(t, backend, 0, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	for attempt := range 100 { // a queue with room must not win over the ended context
		key := fmt.Sprint("key ", attempt)
		require.ErrorIs(t, synchronized.Set(ctx, key, "value"), context.Canceled)
		requireNotLocal(t, synchronized, key)
		require.ErrorIs(t, synchronized.Remove(ctx, key), context.Canceled)
		require.ErrorIs(t, synchronized.RemovePrefix(ctx, key), context.Canceled)
	}

	require.NoError(t, synchronized.Close(t.Context()))
	require.Zero(t, writes)
}

func TestSynchronizedAsyncSetThatIsNotQueuedLeavesNoLocalCopy(t *testing.T) {
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

			requireEventuallyNotLocal(t, synchronized, "key")
			require.NoError(t, synchronized.Close(t.Context())) // the log is complete once the queue drained
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

	requireEventuallyNotLocal(t, synchronized, "key")
	require.NoError(t, synchronized.Close(t.Context()))
	require.Contains(t, logs.String(), "cache: background set panicked")
	require.Contains(t, logs.String(), "backend exploded")
}

func requireEventuallyNotLocal(t *testing.T, synchronized *cache.Synchronized[string, string], key string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := synchronized.Get(context.Background(), key)
		return errors.Is(err, errBackend)
	}, 5*time.Second, time.Millisecond, key)
}

// recordingBackend keeps what the writes of a fakeBackend leave in the backend.
type recordingBackend struct {
	mutex     sync.Mutex
	stored    map[string]string
	performed int
}

func recordWrites(backend *fakeBackend) *recordingBackend {
	recording := &recordingBackend{stored: map[string]string{}}
	backend.set = func(_ context.Context, key, value string) error {
		recording.record(func() { recording.stored[key] = value })
		return nil
	}
	backend.remove = func(_ context.Context, key string) error {
		recording.record(func() { delete(recording.stored, key) })
		return nil
	}
	backend.removePrefix = func(_ context.Context, prefix string) error {
		recording.record(func() {
			maps.DeleteFunc(recording.stored, func(key, _ string) bool { return strings.HasPrefix(key, prefix) })
		})
		return nil
	}
	return recording
}

func (r *recordingBackend) record(write func()) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	write()
	r.performed++
}

func (r *recordingBackend) snapshot() (stored map[string]string, performed int) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return maps.Clone(r.stored), r.performed
}

// submitWrites mixes sets, removals and prefix removals of keys, as one caller of many.
func submitWrites(synchronized *cache.Synchronized[string, string], submitter, writes int, keys []string) error {
	ctx := context.Background()
	for write := range writes {
		key := keys[(submitter+write)%len(keys)]
		var err error
		switch write % 5 {
		case 3:
			err = synchronized.Remove(ctx, key)
		case 4:
			err = synchronized.RemovePrefix(ctx, "user:")
		default:
			err = synchronized.Set(ctx, key, fmt.Sprint(submitter, "/", write))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func TestSynchronizedAsyncWritesFromConcurrentCallersLeaveLocalAndBackendAlike(t *testing.T) {
	const (
		submitters = 8
		writesEach = 200
	)
	keys := []string{"user:1", "user:2", "group:1"}
	backend := newFakeBackend() // emits no events, so only the queue order shapes both sides
	recording := recordWrites(backend)
	synchronized := newAsync(t, backend, 4, nil)

	failures := make(chan error, submitters)
	var submitting sync.WaitGroup
	for submitter := range submitters {
		submitting.Go(func() { failures <- submitWrites(synchronized, submitter, writesEach, keys) })
	}
	submitting.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		_, performed := recording.snapshot()
		return performed == submitters*writesEach
	}, 5*time.Second, time.Millisecond)

	stored, _ := recording.snapshot()
	for _, key := range keys {
		if value, held := stored[key]; held {
			requireLocal(t, synchronized, key, value)
		} else {
			requireNotLocal(t, synchronized, key)
		}
	}
}

func TestSynchronizedWritesWaitingDuringCloseReturnErrClosed(t *testing.T) {
	backend := newFakeBackend()
	started, release := blockingSet(backend)
	synchronized := newAsync(t, backend, 1, nil)
	t.Cleanup(release)
	require.NoError(t, synchronized.Set(t.Context(), "running", "value"))
	<-started
	require.NoError(t, synchronized.Set(t.Context(), "queued", "value"))

	// The first waits for room in the queue, the second for the first to finish submitting.
	waiting := make(chan error, 2)
	for _, key := range []string{"waiting for room", "waiting to submit"} {
		go func() { waiting <- synchronized.Set(context.Background(), key, "value") }()
	}
	require.Eventually(t, func() bool { return goroutinesIn("setInBackground") == 2 }, time.Second, time.Millisecond)

	closed := make(chan error, 1)
	go func() { closed <- synchronized.Close(context.Background()) }()

	require.ErrorIs(t, <-waiting, cache.ErrClosed)
	require.ErrorIs(t, <-waiting, cache.ErrClosed)
	release()
	require.NoError(t, <-closed)
}

func goroutinesIn(function string) int {
	stacks := make([]byte, 1<<20)
	return strings.Count(string(stacks[:runtime.Stack(stacks, true)]), function+"(")
}

// TestSynchronizedAsyncWriteHonorsItsContextBehindAWaitingWrite proves that a write returns its
// context's error when the queue stays full, also while another caller's write waits for room.
func TestSynchronizedAsyncWriteHonorsItsContextBehindAWaitingWrite(t *testing.T) {
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

	written := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		written <- synchronized.Remove(ctx, "other")
	}()

	select {
	case err := <-written:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		release()
		require.Fail(t, "the write ignored its deadline behind another write waiting for room", "it returned %v only after the hanging write was released", <-written)
	}
	release()
	require.NoError(t, <-waiting)
}
