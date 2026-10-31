package subscription_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/govengers/cache/internal/subscription"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

const (
	channel           = "events"
	heartbeatInterval = 50 * time.Millisecond
	// slowHeartbeatInterval is for tests that assert a subscription is not renewed: its limit of
	// three intervals (1.5s) leaves room for a slow test machine.
	slowHeartbeatInterval = 500 * time.Millisecond
	waitDeadline          = 5 * time.Second
	pollInterval          = 10 * time.Millisecond
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// lockedBuffer collects log output written by the subscription goroutine while the test reads it.
type lockedBuffer struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.String()
}

type subscriber struct {
	subscriptions chan struct{}
	messages      chan string
	logged        *lockedBuffer
	stop          func()
}

// keep runs subscription.Keep against the Redis server at address until the test ends or stop is called.
func keep(t *testing.T, address string, reportSubscribed bool, interval time.Duration, hooks ...redis.Hook) *subscriber {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: address})
	for _, hook := range hooks {
		client.AddHook(hook)
	}
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	keeper := &subscriber{
		subscriptions: make(chan struct{}, 100),
		messages:      make(chan string, 100),
		logged:        &lockedBuffer{},
	}
	parameters := &subscription.Parameters{
		Client:            client,
		Channel:           channel,
		Logger:            slog.New(slog.NewTextHandler(keeper.logged, nil)),
		HeartbeatInterval: interval,
		OnMessage:         func(payload string) { keeper.messages <- payload },
	}
	if reportSubscribed {
		parameters.OnSubscribed = func() { keeper.subscriptions <- struct{}{} }
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		subscription.Keep(ctx, parameters)
	}()
	keeper.stop = func() {
		cancel()
		select {
		case <-done:
		case <-time.After(waitDeadline):
			t.Fatal("Keep did not return after its context ended")
		}
	}
	t.Cleanup(keeper.stop)
	return keeper
}

func receive[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(waitDeadline):
		t.Fatal("nothing received")
		var zero T
		return zero
	}
}

func TestKeepDeliversMessagesInOrder(t *testing.T) {
	server := miniredis.RunT(t)
	keeper := keep(t, server.Addr(), false, heartbeatInterval)
	require.Eventually(t, func() bool { return server.PubSubNumSub(channel)[channel] == 1 }, waitDeadline, pollInterval)

	server.Publish(channel, "first")
	server.Publish(channel, "second")

	require.Equal(t, "first", receive(t, keeper.messages))
	require.Equal(t, "second", receive(t, keeper.messages))
}

func TestKeepStopsQuietlyWhenContextEnds(t *testing.T) {
	server := miniredis.RunT(t)
	keeper := keep(t, server.Addr(), true, heartbeatInterval)
	receive(t, keeper.subscriptions)

	keeper.stop()

	require.Empty(t, keeper.logged.String())
	require.Eventually(t, func() bool { return server.PubSubNumSub(channel)[channel] == 0 }, waitDeadline, pollInterval)
}

func TestKeepResubscribesAfterDroppedConnection(t *testing.T) {
	server := miniredis.RunT(t)
	keeper := keep(t, server.Addr(), true, heartbeatInterval)
	receive(t, keeper.subscriptions)
	server.Publish(channel, "before")
	require.Equal(t, "before", receive(t, keeper.messages))

	server.Close()
	require.NoError(t, server.Restart())

	receive(t, keeper.subscriptions) // resubscribed
	server.Publish(channel, "after")
	require.Equal(t, "after", receive(t, keeper.messages))
	require.Contains(t, keeper.logged.String(), "redis: subscription failed, resubscribing")
}

func TestKeepRetriesUntilFirstSubscription(t *testing.T) {
	server := miniredis.RunT(t)
	address := server.Addr()
	server.Close()
	keeper := keep(t, address, true, heartbeatInterval)
	require.Eventually(t, func() bool {
		return strings.Count(keeper.logged.String(), "redis: subscribing failed") >= 2
	}, waitDeadline, pollInterval)

	require.NoError(t, server.Restart())

	receive(t, keeper.subscriptions)
	server.Publish(channel, "first")
	require.Equal(t, "first", receive(t, keeper.messages))
}

// heartbeatHook sees the heartbeat pipelines (PUBLISH + INFO replication) of the client. It
// counts the heartbeats, drops them while drop is set and drops the dropOnlyNumber-th one; a
// dropped PUBLISH looks successful to Keep. While primaryID is set, INFO reports it as the
// primary's replication ID (miniredis does not support INFO replication).
type heartbeatHook struct {
	drop           atomic.Bool
	dropOnlyNumber int64
	published      atomic.Int64
	primaryID      atomic.Pointer[string]
}

func (*heartbeatHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (*heartbeatHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (h *heartbeatHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		forwarded := make([]redis.Cmder, 0, len(commands))
		for _, command := range commands {
			if isHeartbeat(command) {
				number := h.published.Add(1)
				if h.drop.Load() || number == h.dropOnlyNumber {
					continue
				}
			}
			forwarded = append(forwarded, command)
		}
		err := next(ctx, forwarded)
		primaryID := h.primaryID.Load()
		if primaryID == nil {
			return err
		}
		for _, command := range commands {
			if info, isInfo := command.(*redis.StringCmd); isInfo && command.Name() == "info" {
				info.SetErr(nil)
				info.SetVal("# Replication\r\nrole:master\r\nmaster_replid:" + *primaryID + "\r\n")
			}
		}
		return nil
	}
}

func isHeartbeat(command redis.Cmder) bool {
	arguments := command.Args()
	return command.Name() == "publish" && len(arguments) > 1 && strings.Contains(fmt.Sprint(arguments[1]), ":heartbeat:")
}

func TestKeepDoesNotHandHeartbeatsToOnMessage(t *testing.T) {
	server := miniredis.RunT(t)
	hook := &heartbeatHook{}
	keeper := keep(t, server.Addr(), true, slowHeartbeatInterval, hook)
	receive(t, keeper.subscriptions)
	require.Eventually(t, func() bool { return hook.published.Load() >= 3 }, waitDeadline, pollInterval)

	server.Publish(channel, "only")

	require.Equal(t, "only", receive(t, keeper.messages))
	require.Empty(t, keeper.messages)
	require.Empty(t, keeper.subscriptions, "a subscription receiving its heartbeats is not renewed")
	require.NotContains(t, keeper.logged.String(), "missed its heartbeats")
}

func TestKeepResubscribesAfterMissedHeartbeats(t *testing.T) {
	server := miniredis.RunT(t)
	hook := &heartbeatHook{}
	hook.drop.Store(true)
	keeper := keep(t, server.Addr(), true, heartbeatInterval, hook)
	receive(t, keeper.subscriptions)

	receive(t, keeper.subscriptions) // renewed

	require.Positive(t, hook.published.Load())
	require.Contains(t, keeper.logged.String(), "redis: subscription missed its heartbeats, resubscribing")
	hook.drop.Store(false)
	require.Eventually(t, func() bool { return server.PubSubNumSub(channel)[channel] == 1 }, waitDeadline, pollInterval)
	server.Publish(channel, "after")
	require.Equal(t, "after", receive(t, keeper.messages))
}

func TestKeepResubscribesAfterLosingOneHeartbeat(t *testing.T) {
	server := miniredis.RunT(t)
	// The first heartbeat is lost and the second arrives after one interval, a full second before
	// the three intervals after which missing heartbeats renew the subscription.
	hook := &heartbeatHook{dropOnlyNumber: 1}
	keeper := keep(t, server.Addr(), true, slowHeartbeatInterval, hook)
	receive(t, keeper.subscriptions)

	receive(t, keeper.subscriptions) // renewed

	require.Contains(t, keeper.logged.String(), "redis: subscription lost a heartbeat, resubscribing")
	require.NotContains(t, keeper.logged.String(), "missed its heartbeats")
	require.Eventually(t, func() bool { return server.PubSubNumSub(channel)[channel] == 1 }, waitDeadline, pollInterval)
	server.Publish(channel, "after")
	require.Equal(t, "after", receive(t, keeper.messages))
}

func TestKeepResubscribesWhenThePrimaryChanges(t *testing.T) {
	server := miniredis.RunT(t)
	hook := &heartbeatHook{}
	hook.primaryID.Store(new("first-primary"))
	keeper := keep(t, server.Addr(), true, slowHeartbeatInterval, hook)
	receive(t, keeper.subscriptions)
	require.Eventually(t, func() bool { return hook.published.Load() >= 3 }, waitDeadline, pollInterval)
	require.Empty(t, keeper.subscriptions, "an unchanged primary does not renew the subscription")

	hook.primaryID.Store(new("second-primary"))

	receive(t, keeper.subscriptions) // renewed
	require.Contains(t, keeper.logged.String(), "redis: the primary changed, resubscribing")
	require.Eventually(t, func() bool { return server.PubSubNumSub(channel)[channel] == 1 }, waitDeadline, pollInterval)
	server.Publish(channel, "after")
	require.Equal(t, "after", receive(t, keeper.messages))
	require.Empty(t, keeper.subscriptions, "the renewed subscription takes the new primary as its own")
}

// unconfirmedSubscribeHook swallows every SUBSCRIBE the client writes, so the server never
// confirms the subscription, like on a connection that died silently.
type unconfirmedSubscribeHook struct{}

func (unconfirmedSubscribeHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		connection, err := next(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return subscribeSwallowingConnection{Conn: connection}, nil
	}
}

func (unconfirmedSubscribeHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (unconfirmedSubscribeHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

type subscribeSwallowingConnection struct {
	net.Conn
}

func (c subscribeSwallowingConnection) Write(data []byte) (int, error) {
	if bytes.Contains(bytes.ToLower(data), []byte("\r\nsubscribe\r\n")) {
		return len(data), nil
	}
	return c.Conn.Write(data)
}

func TestKeepGivesUpOnAnUnconfirmedSubscription(t *testing.T) {
	server := miniredis.RunT(t)
	keeper := keep(t, server.Addr(), true, heartbeatInterval, unconfirmedSubscribeHook{})

	require.Eventually(t, func() bool {
		return strings.Count(keeper.logged.String(), "redis: subscribing failed") >= 2
	}, waitDeadline, pollInterval)
	require.Contains(t, keeper.logged.String(), "timeout")
	require.Empty(t, keeper.subscriptions)
}
