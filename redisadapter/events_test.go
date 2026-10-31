package redisadapter_test

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"

	"github.com/alicebob/miniredis/v2"
	"github.com/govengers/cache"
	"github.com/govengers/cache/redisadapter"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// collect registers a handler on store that hands every event to the returned channel. The
// buffer is large enough that the receiving goroutine never blocks on it.
func collect[K comparable, V any](store *redisadapter.Store[K, V]) <-chan cache.Event[K, V] {
	events := make(chan cache.Event[K, V], 256)
	store.OnEvent(func(event cache.Event[K, V]) { events <- event })
	return events
}

func receive[K comparable, V any](t *testing.T, events <-chan cache.Event[K, V]) cache.Event[K, V] {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(waitDeadline):
		t.Fatal("no event received")
		return cache.Event[K, V]{}
	}
}

// awaitSubscription waits until store receives events. The EventReset of its first subscription
// has then been delivered, so handlers registered afterwards do not see it.
func awaitSubscription[K comparable, V any](t *testing.T, store *redisadapter.Store[K, V]) {
	t.Helper()
	require.NoError(t, store.AwaitSubscription(withTimeout(t, waitDeadline)))
}

func withTimeout(t *testing.T, timeout time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	t.Cleanup(cancel)
	return ctx
}

// newEventStore returns a subscribed store on the event channel, with its own client.
func newEventStore[K comparable, V any](t *testing.T, server *miniredis.Miniredis, options *redisadapter.Options[K]) *redisadapter.Store[K, V] {
	t.Helper()
	options.Client = newClient(t, server)
	options.EventChannel = eventChannel
	store := newStore[K, V](t, options)
	awaitSubscription(t, store)
	return store
}

func newProfileStore(t *testing.T, server *miniredis.Miniredis, logger *slog.Logger) *redisadapter.Store[string, profile] {
	t.Helper()
	return newEventStore[string, profile](t, server, &redisadapter.Options[string]{
		KeyCodec:  cache.StringKeyCodec{},
		KeyPrefix: "users",
		Logger:    logger,
	})
}

func TestEventsReachEveryStoreOnTheChannel(t *testing.T) {
	server := miniredis.RunT(t)
	first := newProfileStore(t, server, nil)
	second := newProfileStore(t, server, nil)
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

func TestEventsFollowTheOrderRedisAppliedTheChanges(t *testing.T) {
	const setsPerStore = 50
	server := miniredis.RunT(t)
	newStringStore := func() *redisadapter.Store[string, string] {
		return newEventStore[string, string](t, server, &redisadapter.Options[string]{KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "items"})
	}
	writers := []*redisadapter.Store[string, string]{newStringStore(), newStringStore()}
	observer := newStringStore()
	events := collect(observer)
	ctx := t.Context()

	var writing sync.WaitGroup
	for writerIndex, writer := range writers {
		writing.Go(func() {
			for setIndex := range setsPerStore {
				if err := writer.Set(ctx, "shared", fmt.Sprintf("writer %d set %d", writerIndex, setIndex)); err != nil {
					t.Error(err)
				}
			}
		})
	}
	writing.Wait()

	var localValue string
	for range len(writers) * setsPerStore {
		event := receive(t, events)
		require.Equal(t, cache.EventSet, event.Type)
		localValue = event.Value
	}
	redisValue, err := observer.Get(ctx, "shared")
	require.NoError(t, err)
	require.Equal(t, redisValue, localValue)
}

func TestStoreWithoutEventChannelPublishesNothing(t *testing.T) {
	server := miniredis.RunT(t)
	client := newClient(t, server)
	store := newStore[string, string](t, &redisadapter.Options[string]{Client: client, KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "items"})
	store.OnEvent(func(cache.Event[string, string]) { t.Error("handler called without an event channel") })
	ctx := t.Context()
	everything := client.PSubscribe(ctx, "*")
	t.Cleanup(func() { require.NoError(t, everything.Close()) })
	_, err := everything.Receive(ctx)
	require.NoError(t, err)

	require.NoError(t, store.Set(ctx, "alice", "value"))
	require.NoError(t, store.Remove(ctx, "alice"))
	require.NoError(t, store.RemovePrefix(ctx, ""))

	// Redis delivers in order, so the marker is the first message only if nothing came before.
	server.Publish("marker", "end")
	message, err := everything.ReceiveMessage(withTimeout(t, waitDeadline))
	require.NoError(t, err)
	require.Equal(t, "marker", message.Channel)
}

func TestEventValueAgreesWithGet(t *testing.T) {
	testCases := []struct {
		name  string
		value []string
	}{
		{name: "nil slice", value: nil},
		{name: "empty slice", value: []string{}},
		{name: "filled slice", value: []string{"a", "b"}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := miniredis.RunT(t)
			store := newEventStore[string, []string](t, server, &redisadapter.Options[string]{KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "lists"})
			events := collect(store)

			require.NoError(t, store.Set(t.Context(), "list", testCase.value))

			event := receive(t, events)
			value, err := store.Get(t.Context(), "list")
			require.NoError(t, err)
			require.Equal(t, value, event.Value)
			require.Equal(t, testCase.value, value)
		})
	}
}

type point struct {
	X, Y int
}

// pointKeyCodec encodes a point as "x/y", which differs from its MessagePack encoding.
type pointKeyCodec struct{}

func (pointKeyCodec) Encode(key point) string {
	return fmt.Sprintf("%d/%d", key.X, key.Y)
}

func (pointKeyCodec) Decode(encoded string) (point, error) {
	var key point
	if _, err := fmt.Sscanf(encoded, "%d/%d", &key.X, &key.Y); err != nil {
		return point{}, fmt.Errorf("decode point %q: %w", encoded, err)
	}
	return key, nil
}

func TestEventKeysTravelInTheKeyCodecEncoding(t *testing.T) {
	server := miniredis.RunT(t)
	store := newEventStore[point, string](t, server, &redisadapter.Options[point]{KeyCodec: pointKeyCodec{}, KeyPrefix: "points"})
	events := collect(store)
	ctx := t.Context()
	origin := point{X: 3, Y: -4}

	require.NoError(t, store.Set(ctx, origin, "here"))
	require.NoError(t, store.Remove(ctx, origin))

	require.Equal(t, cache.Event[point, string]{Type: cache.EventSet, Key: origin, Value: "here"}, receive(t, events))
	require.Equal(t, cache.Event[point, string]{Type: cache.EventRemove, Key: origin}, receive(t, events))
}

func TestEventsFromTheWire(t *testing.T) {
	testCases := []struct {
		name           string
		payload        func(t *testing.T) string
		expected       cache.Event[int, string]
		expectedLogged bool
	}{
		{
			name: "set",
			payload: func(t *testing.T) string {
				return encode(t, map[string]any{"t": 1, "k": "7", "v": msgpack.RawMessage(encode(t, "seven"))})
			},
			expected: cache.Event[int, string]{Type: cache.EventSet, Key: 7, Value: "seven"},
		},
		{
			name:     "remove",
			payload:  func(t *testing.T) string { return encode(t, map[string]any{"t": 2, "k": "7"}) },
			expected: cache.Event[int, string]{Type: cache.EventRemove, Key: 7},
		},
		{
			name:     "remove prefix",
			payload:  func(t *testing.T) string { return encode(t, map[string]any{"t": 3, "p": "1"}) },
			expected: cache.Event[int, string]{Type: cache.EventRemovePrefix, Prefix: "1"},
		},
		{
			name:     "reset",
			payload:  func(t *testing.T) string { return encode(t, map[string]any{"t": 4}) },
			expected: cache.Event[int, string]{Type: cache.EventReset},
		},
		{
			name:           "not MessagePack",
			payload:        func(*testing.T) string { return "\xc1" },
			expected:       cache.Event[int, string]{Type: cache.EventReset},
			expectedLogged: true,
		},
		{
			name:           "undecodable key",
			payload:        func(t *testing.T) string { return encode(t, map[string]any{"t": 2, "k": "seven"}) },
			expected:       cache.Event[int, string]{Type: cache.EventReset},
			expectedLogged: true,
		},
		{
			name:           "unknown type",
			payload:        func(t *testing.T) string { return encode(t, map[string]any{"t": 99}) },
			expected:       cache.Event[int, string]{Type: cache.EventReset},
			expectedLogged: true,
		},
		{
			name:           "missing type",
			payload:        func(t *testing.T) string { return encode(t, map[string]any{"k": "7"}) },
			expected:       cache.Event[int, string]{Type: cache.EventReset},
			expectedLogged: true,
		},
		{
			name: "undecodable value",
			payload: func(t *testing.T) string {
				return encode(t, map[string]any{"t": 1, "k": "7", "v": msgpack.RawMessage("\xc1")})
			},
			expected:       cache.Event[int, string]{Type: cache.EventReset},
			expectedLogged: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := miniredis.RunT(t)
			logger, logged := newLogger()
			store := newEventStore[int, string](t, server, &redisadapter.Options[int]{KeyCodec: cache.IntKeyCodec{}, KeyPrefix: "numbers", Logger: logger})
			events := collect(store)

			server.Publish(eventChannel, testCase.payload(t))

			require.Equal(t, testCase.expected, receive(t, events))
			require.Equal(t, testCase.expectedLogged, strings.Contains(logged.String(), "redisadapter: undecodable event, resetting"), logged.String())
		})
	}
}

// countRemovePrefixEvents removes key as a marker and counts the EventRemovePrefix events for
// prefix that arrive before the marker's EventRemove.
func countRemovePrefixEvents(t *testing.T, store *redisadapter.Store[string, string], events <-chan cache.Event[string, string], prefix, marker string) int {
	t.Helper()
	require.NoError(t, store.Remove(t.Context(), marker))
	count := 0
	for {
		event := receive(t, events)
		if event.Type == cache.EventRemove && event.Key == marker {
			return count
		}
		require.Equal(t, cache.Event[string, string]{Type: cache.EventRemovePrefix, Prefix: prefix}, event)
		count++
	}
}

func TestRemovePrefixRemovesManyKeysInChunks(t *testing.T) {
	const keyCount = 2500 // one SCAN page from miniredis, three scripts of at most 1000 keys
	server := miniredis.RunT(t)
	store := newEventStore[string, string](t, server, &redisadapter.Options[string]{KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "items"})
	events := collect(store)
	for index := range keyCount {
		require.NoError(t, server.Set(fmt.Sprintf("items:key:%d", index), encode(t, "value")))
	}
	require.NoError(t, server.Set("other:key:1", encode(t, "outside")))

	require.NoError(t, store.RemovePrefix(t.Context(), "key:"))

	require.Equal(t, []string{"other:key:1"}, server.Keys())
	require.Equal(t, 3+1, countRemovePrefixEvents(t, store, events, "key:", "marker"), "one event per script and a final one")
}

func TestRemovePrefixAnnouncesEveryPage(t *testing.T) {
	server := miniredis.RunT(t)
	store := newEventStore[string, string](t, server, &redisadapter.Options[string]{KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "items"})
	pagingStore := newStore[string, string](t, &redisadapter.Options[string]{
		Client:       newClient(t, server, &pagingHook{}),
		KeyCodec:     cache.StringKeyCodec{},
		KeyPrefix:    "items",
		EventChannel: eventChannel,
	})
	events := collect(store)
	for _, key := range []string{"a", "b", "c"} {
		require.NoError(t, server.Set("items:"+key, encode(t, "value")))
	}

	require.NoError(t, pagingStore.RemovePrefix(t.Context(), ""))

	require.Empty(t, server.Keys())
	require.Equal(t, 3+1, countRemovePrefixEvents(t, store, events, "", "marker"), "one event per page and a final one")
}

func TestRemovePrefixAnnouncesEvenWhenNothingMatched(t *testing.T) {
	server := miniredis.RunT(t)
	store := newEventStore[string, string](t, server, &redisadapter.Options[string]{KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "items"})
	events := collect(store)

	require.NoError(t, store.RemovePrefix(t.Context(), "missing"))

	require.Equal(t, 1, countRemovePrefixEvents(t, store, events, "missing", "marker"))
}

func TestRemovePrefixAnnouncesRemovedPagesWhenALaterPageFails(t *testing.T) {
	server := miniredis.RunT(t)
	store := newEventStore[string, string](t, server, &redisadapter.Options[string]{KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "items"})
	// The first page's script passes, the second page's fails.
	failingStore := newStore[string, string](t, &redisadapter.Options[string]{
		Client:       newClient(t, server, &pagingHook{}, &failingHook{command: "evalsha", passing: 1}),
		KeyCodec:     cache.StringKeyCodec{},
		KeyPrefix:    "items",
		EventChannel: eventChannel,
	})
	events := collect(store)
	for _, key := range []string{"a", "b", "c"} {
		require.NoError(t, server.Set("items:"+key, encode(t, "value")))
	}

	err := failingStore.RemovePrefix(t.Context(), "")

	require.ErrorContains(t, err, errInjected.Error())
	require.NotErrorIs(t, err, errInjected)
	require.Equal(t, []string{"items:b", "items:c"}, server.Keys())
	require.Equal(t, 1+1, countRemovePrefixEvents(t, store, events, "", "marker"), "the removed page and the final event")
}

// registerHeavyHandler registers a handler holding a large allocation and returns a weak pointer
// to it and the handler's unregister function.
func registerHeavyHandler(store *redisadapter.Store[string, string]) (weak.Pointer[[1 << 20]byte], func()) {
	heavy := new([1 << 20]byte)
	unregister := store.OnEvent(func(cache.Event[string, string]) { heavy[0]++ })
	return weak.Make(heavy), unregister
}

func TestUnregisteredHandlerIsNotRetained(t *testing.T) {
	testCases := []struct {
		name     string
		position int
	}{
		{name: "first handler", position: 0},
		{name: "middle handler", position: 1},
		{name: "last handler", position: 2},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := miniredis.RunT(t)
			store := newStore[string, string](t, &redisadapter.Options[string]{Client: newClient(t, server), KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "items"})
			var heavy weak.Pointer[[1 << 20]byte]
			var unregister func()
			for position := range 3 {
				if position == testCase.position {
					heavy, unregister = registerHeavyHandler(store)
				} else {
					store.OnEvent(func(cache.Event[string, string]) {})
				}
			}

			unregister()

			require.Eventually(t, func() bool {
				runtime.GC()
				return heavy.Value() == nil
			}, waitDeadline, pollInterval)
		})
	}
}

func TestOnEventUnregister(t *testing.T) {
	server := miniredis.RunT(t)
	store := newEventStore[string, string](t, server, &redisadapter.Options[string]{KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "items"})
	before := collect(store)
	unregistered := make(chan cache.Event[string, string], 16)
	unregister := store.OnEvent(func(event cache.Event[string, string]) { unregistered <- event })
	after := collect(store)
	ctx := t.Context()

	require.NoError(t, store.Set(ctx, "alice", "first"))
	require.Equal(t, "first", receive(t, after).Value)
	require.Equal(t, "first", receive(t, unregistered).Value)

	unregister()
	unregister() // a second call changes nothing
	require.NoError(t, store.Set(ctx, "alice", "second"))

	require.Equal(t, "first", receive(t, before).Value)
	require.Equal(t, "second", receive(t, before).Value)
	// Handlers run in registration order, so the handler after it has seen the event once the
	// unregistered one would have.
	require.Equal(t, "second", receive(t, after).Value)
	require.Empty(t, unregistered)
}

func TestEventResetAfterResubscription(t *testing.T) {
	server := miniredis.RunT(t)
	logger, logged := newLogger()
	store := newProfileStore(t, server, logger)
	events := collect(store)

	server.Close()
	require.NoError(t, server.Restart())

	require.Equal(t, cache.Event[string, profile]{Type: cache.EventReset}, receive(t, events))
	require.Contains(t, logged.String(), "redis: subscription failed, resubscribing")
	require.NoError(t, store.Set(t.Context(), "alice", profile{Name: "Alice"}))
	require.Equal(t, cache.Event[string, profile]{Type: cache.EventSet, Key: "alice", Value: profile{Name: "Alice"}}, receive(t, events))
}

func TestPanickingHandlerDoesNotStopTheOthers(t *testing.T) {
	server := miniredis.RunT(t)
	logger, logged := newLogger()
	store := newProfileStore(t, server, logger)
	store.OnEvent(func(cache.Event[string, profile]) { panic("broken handler") })
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
			store := newStore[string, string](t, &redisadapter.Options[string]{
				Client:       newClient(t, server),
				KeyCodec:     cache.StringKeyCodec{},
				KeyPrefix:    "items",
				EventChannel: testCase.eventChannel,
			})
			if testCase.eventChannel != "" {
				awaitSubscription(t, store)
			}
			if testCase.serverDown {
				server.Close()
			}

			require.NoError(t, store.Close(withTimeout(t, waitDeadline)))
			require.NoError(t, store.Close(withTimeout(t, waitDeadline)))
		})
	}
}

// blockingHeartbeatHook holds every heartbeat pipeline while blocking is set, ignoring its
// context, until release is closed.
type blockingHeartbeatHook struct {
	blocking atomic.Bool
	blocked  chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (*blockingHeartbeatHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (*blockingHeartbeatHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (h *blockingHeartbeatHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		if h.blocking.Load() {
			h.once.Do(func() { close(h.blocked) })
			<-h.release
		}
		return next(ctx, commands)
	}
}

func TestCloseReturnsTheContextErrorWhenReceivingDoesNotStop(t *testing.T) {
	server := miniredis.RunT(t)
	hook := &blockingHeartbeatHook{blocked: make(chan struct{}), release: make(chan struct{})}
	store, err := redisadapter.New[string, string](&redisadapter.Options[string]{
		Client:            newClient(t, server, hook),
		KeyCodec:          cache.StringKeyCodec{},
		KeyPrefix:         "items",
		EventChannel:      eventChannel,
		HeartbeatInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	awaitSubscription(t, store)
	hook.blocking.Store(true)
	<-hook.blocked

	err = store.Close(withTimeout(t, 50*time.Millisecond))

	require.ErrorIs(t, err, context.DeadlineExceeded)
	close(hook.release)
	require.NoError(t, store.Close(withTimeout(t, waitDeadline)))
}

// panickingOnceHook panics in the first heartbeat pipeline after armed is set, inside the
// store's receiving goroutine.
type panickingOnceHook struct {
	armed    atomic.Bool
	panicked atomic.Bool
}

func (*panickingOnceHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (*panickingOnceHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (h *panickingOnceHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		if h.armed.Load() && h.panicked.CompareAndSwap(false, true) {
			panic("broken heartbeat")
		}
		return next(ctx, commands)
	}
}

func TestReceivingRestartsAfterAPanic(t *testing.T) {
	server := miniredis.RunT(t)
	logger, logged := newLogger()
	hook := &panickingOnceHook{}
	store := newStore[string, string](t, &redisadapter.Options[string]{
		Client:            newClient(t, server, hook),
		KeyCodec:          cache.StringKeyCodec{},
		KeyPrefix:         "items",
		EventChannel:      eventChannel,
		HeartbeatInterval: 50 * time.Millisecond,
		Logger:            logger,
	})
	awaitSubscription(t, store)
	events := collect(store)

	hook.armed.Store(true)

	require.Equal(t, cache.Event[string, string]{Type: cache.EventReset}, receive(t, events))
	require.True(t, hook.panicked.Load())
	require.Contains(t, logged.String(), "redisadapter: receiving events panicked")
	require.Contains(t, logged.String(), "broken heartbeat")
	require.NoError(t, store.Set(t.Context(), "alice", "value"))
	require.Equal(t, cache.Event[string, string]{Type: cache.EventSet, Key: "alice", Value: "value"}, receive(t, events))
}

// heartbeatDroppingHook drops the heartbeats the subscription publishes to itself, which go out
// in a pipeline with INFO replication.
type heartbeatDroppingHook struct{}

func (heartbeatDroppingHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (heartbeatDroppingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (heartbeatDroppingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		forwarded := make([]redis.Cmder, 0, len(commands))
		for _, command := range commands {
			arguments := command.Args()
			if command.Name() == "publish" && len(arguments) > 1 && strings.Contains(fmt.Sprint(arguments[1]), ":heartbeat:") {
				continue
			}
			forwarded = append(forwarded, command)
		}
		return next(ctx, forwarded)
	}
}

func TestEventResetAfterMissedHeartbeats(t *testing.T) {
	server := miniredis.RunT(t)
	logger, logged := newLogger()
	store := newStore[string, string](t, &redisadapter.Options[string]{
		Client:            newClient(t, server, heartbeatDroppingHook{}),
		KeyCodec:          cache.StringKeyCodec{},
		KeyPrefix:         "items",
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
	store := newStore[string, string](t, &redisadapter.Options[string]{
		Client:       client,
		KeyCodec:     cache.StringKeyCodec{},
		KeyPrefix:    "items",
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
			store := newStore[string, string](t, &redisadapter.Options[string]{
				Client:       client,
				KeyCodec:     cache.StringKeyCodec{},
				KeyPrefix:    "items",
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
