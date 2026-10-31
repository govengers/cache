package redisadapter_test

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/govengers/cache"
	"github.com/govengers/cache/redisadapter"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// collect registers a handler on store that hands every event to the returned channel. The
// buffer is large enough that the receiving goroutine never blocks on it.
func collect[V any](store *redisadapter.Store[string, V]) <-chan cache.Event[string, V] {
	events := make(chan cache.Event[string, V], 64)
	store.OnEvent(func(event cache.Event[string, V]) { events <- event })
	return events
}

func receive[V any](t *testing.T, events <-chan cache.Event[string, V]) cache.Event[string, V] {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(waitDeadline):
		t.Fatal("no event received")
		return cache.Event[string, V]{}
	}
}

// awaitSubscription waits until store receives events. The EventReset of its first subscription
// has then been delivered, so handlers registered afterwards do not see it.
func awaitSubscription[V any](t *testing.T, store *redisadapter.Store[string, V]) {
	t.Helper()
	require.NoError(t, store.AwaitSubscription(withTimeout(t, waitDeadline)))
}

func newEventStore(t *testing.T, server *miniredis.Miniredis, logger *slog.Logger) *redisadapter.Store[string, profile] {
	t.Helper()
	return newStore[string, profile](t, &redisadapter.StoreOptions[string]{
		Client:       newClient(t, server),
		KeyCodec:     cache.StringKeyCodec{},
		KeyPrefix:    "users",
		EventChannel: eventChannel,
		Logger:       logger,
	})
}

func TestEventsReachEveryStoreOnTheChannel(t *testing.T) {
	server := miniredis.RunT(t)
	first := newEventStore(t, server, nil)
	second := newEventStore(t, server, nil)
	awaitSubscription(t, first)
	awaitSubscription(t, second)
	firstEvents, secondEvents := collect(first), collect(second)
	ctx := t.Context()
	alice := profile{Name: "Alice", Age: 42}

	require.NoError(t, first.Set(ctx, "alice", alice))
	require.NoError(t, second.Remove(ctx, "alice"))
	require.NoError(t, first.RemovePrefix(ctx, "al"))

	for _, events := range []<-chan cache.Event[string, profile]{firstEvents, secondEvents} {
		require.Equal(t, cache.Event[string, profile]{Type: cache.EventSet, Key: "alice", Value: alice}, receive(t, events))
		require.Equal(t, cache.Event[string, profile]{Type: cache.EventRemove, Key: "alice"}, receive(t, events))
		require.Equal(t, cache.Event[string, profile]{Type: cache.EventRemovePrefix, Prefix: "al"}, receive(t, events))
	}
}

// commandRecorder records the name of every command sent through the client.
type commandRecorder struct {
	mutex    sync.Mutex
	commands []string
}

func (*commandRecorder) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (r *commandRecorder) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		r.mutex.Lock()
		r.commands = append(r.commands, command.Name())
		r.mutex.Unlock()
		return next(ctx, command)
	}
}

func (*commandRecorder) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestStoreWithoutEventChannelPublishesNothing(t *testing.T) {
	server := miniredis.RunT(t)
	recorder := &commandRecorder{}
	store := newStore[string, string](t, &redisadapter.StoreOptions[string]{
		Client:   newClient(t, server, recorder),
		KeyCodec: cache.StringKeyCodec{},
	})
	store.OnEvent(func(cache.Event[string, string]) { t.Error("handler called without an event channel") })
	ctx := t.Context()

	require.NoError(t, store.Set(ctx, "alice", "value"))
	require.NoError(t, store.Remove(ctx, "alice"))
	require.NoError(t, store.RemovePrefix(ctx, ""))

	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	require.NotEmpty(t, recorder.commands)
	require.False(t, slices.Contains(recorder.commands, "publish"), "commands sent: %v", recorder.commands)
}

func TestEventResetAfterResubscription(t *testing.T) {
	server := miniredis.RunT(t)
	logger, logged := newLogger()
	store := newEventStore(t, server, logger)
	awaitSubscription(t, store)
	events := collect(store)

	server.Close()
	require.NoError(t, server.Restart())

	require.Equal(t, cache.Event[string, profile]{Type: cache.EventReset}, receive(t, events))
	require.Contains(t, logged.String(), "redis: subscription failed, resubscribing")
	require.NoError(t, store.Set(t.Context(), "alice", profile{Name: "Alice"}))
	require.Equal(t, cache.Event[string, profile]{Type: cache.EventSet, Key: "alice", Value: profile{Name: "Alice"}}, receive(t, events))
}

func TestUndecodableEventIsLoggedAndSkipped(t *testing.T) {
	server := miniredis.RunT(t)
	logger, logged := newLogger()
	store := newEventStore(t, server, logger)
	awaitSubscription(t, store)
	events := collect(store)

	server.Publish(eventChannel, "\xc1")
	require.NoError(t, store.Remove(t.Context(), "alice"))

	require.Equal(t, cache.Event[string, profile]{Type: cache.EventRemove, Key: "alice"}, receive(t, events))
	require.Contains(t, logged.String(), "redisadapter: dropped an undecodable event")
}

func TestPanickingHandlerDoesNotStopTheOthers(t *testing.T) {
	server := miniredis.RunT(t)
	logger, logged := newLogger()
	store := newEventStore(t, server, logger)
	store.OnEvent(func(cache.Event[string, profile]) { panic("broken handler") })
	awaitSubscription(t, store)
	events := collect(store)

	require.NoError(t, store.Remove(t.Context(), "alice"))
	require.NoError(t, store.Remove(t.Context(), "bob"))

	require.Equal(t, "alice", receive(t, events).Key)
	require.Equal(t, "bob", receive(t, events).Key)
	require.Contains(t, logged.String(), "redisadapter: event handler panicked")
	require.Contains(t, logged.String(), "broken handler")
}

func TestCloseIsIdempotentAndPrompt(t *testing.T) {
	testCases := []struct {
		name         string
		eventChannel string
		serverDown   bool
	}{
		{name: "receiving events", eventChannel: eventChannel},
		{name: "resubscribing to a server that is down", eventChannel: eventChannel, serverDown: true},
		{name: "without event channel"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := miniredis.RunT(t)
			store := newStore[string, string](t, &redisadapter.StoreOptions[string]{
				Client:       newClient(t, server),
				KeyCodec:     cache.StringKeyCodec{},
				EventChannel: testCase.eventChannel,
			})
			if testCase.eventChannel != "" {
				awaitSubscription(t, store)
			}
			if testCase.serverDown {
				server.Close()
			}

			closed := make(chan struct{})
			go func() {
				defer close(closed)
				store.Close()
				store.Close()
			}()

			select {
			case <-closed:
			case <-time.After(waitDeadline):
				t.Fatal("Close did not return")
			}
		})
	}
}

// heartbeatDroppingHook drops the heartbeats the subscription publishes to itself.
type heartbeatDroppingHook struct{}

func (heartbeatDroppingHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (heartbeatDroppingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		arguments := command.Args()
		if command.Name() == "publish" && len(arguments) > 1 && strings.Contains(fmt.Sprint(arguments[1]), ":heartbeat:") {
			return nil
		}
		return next(ctx, command)
	}
}

func (heartbeatDroppingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestEventResetAfterMissedHeartbeats(t *testing.T) {
	server := miniredis.RunT(t)
	logger, logged := newLogger()
	store := newStore[string, string](t, &redisadapter.StoreOptions[string]{
		Client:            newClient(t, server, heartbeatDroppingHook{}),
		KeyCodec:          cache.StringKeyCodec{},
		EventChannel:      eventChannel,
		HeartbeatInterval: 50 * time.Millisecond,
		Logger:            logger,
	})
	awaitSubscription(t, store)
	events := collect(store)

	require.Equal(t, cache.Event[string, string]{Type: cache.EventReset}, receive(t, events))
	require.Contains(t, logged.String(), "redis: subscription missed its heartbeats, resubscribing")
}

func TestFirstSubscriptionDeliversEventReset(t *testing.T) {
	server := miniredis.RunT(t)
	client := newClient(t, server)
	// With the server down, the handler is registered before the first subscription is up.
	server.Close()
	store := newStore[string, string](t, &redisadapter.StoreOptions[string]{
		Client:       client,
		KeyCodec:     cache.StringKeyCodec{},
		EventChannel: eventChannel,
	})
	events := collect(store)

	require.NoError(t, server.Restart())

	require.Equal(t, cache.Event[string, string]{Type: cache.EventReset}, receive(t, events))
	awaitSubscription(t, store)
	require.NoError(t, store.Set(t.Context(), "alice", "value"))
	require.Equal(t, cache.Event[string, string]{Type: cache.EventSet, Key: "alice", Value: "value"}, receive(t, events))
}

func TestAwaitSubscription(t *testing.T) {
	testCases := []struct {
		name         string
		eventChannel string
		serverDown   bool
		newContext   func(t *testing.T) context.Context
		expected     error
	}{
		{
			name:         "returns once subscribed",
			eventChannel: eventChannel,
			newContext:   func(t *testing.T) context.Context { return withTimeout(t, waitDeadline) },
		},
		{
			name:         "returns the canceled context",
			eventChannel: eventChannel,
			serverDown:   true,
			newContext: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				return ctx
			},
			expected: context.Canceled,
		},
		{
			name:         "returns the exceeded deadline",
			eventChannel: eventChannel,
			serverDown:   true,
			newContext:   func(t *testing.T) context.Context { return withTimeout(t, 50*time.Millisecond) },
			expected:     context.DeadlineExceeded,
		},
		{
			name:       "fails without event channel",
			newContext: func(t *testing.T) context.Context { return t.Context() },
			expected:   redisadapter.ErrNoEventChannel,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := miniredis.RunT(t)
			client := newClient(t, server)
			if testCase.serverDown {
				server.Close()
			}
			store := newStore[string, string](t, &redisadapter.StoreOptions[string]{
				Client:       client,
				KeyCodec:     cache.StringKeyCodec{},
				EventChannel: testCase.eventChannel,
			})

			err := store.AwaitSubscription(testCase.newContext(t))

			if testCase.expected == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, testCase.expected)
		})
	}
}

func withTimeout(t *testing.T, timeout time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	t.Cleanup(cancel)
	return ctx
}
