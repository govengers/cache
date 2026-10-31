//go:build e2e

// Package e2e drives the public API against a real Redis at E2E_REDIS_ADDRESS and a Sentinel
// group at E2E_SENTINEL_ADDRESS, which `just test-e2e` starts with docker compose. Each "instance" is its own Redis client, store and cache, as
// it would be in a separate process.
package e2e

import (
	"context"
	"log/slog"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/govengers/cache"
	"github.com/govengers/cache/redisadapter"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

const propagationDeadline = 5 * time.Second

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type product struct {
	Name       string
	PriceCents int64
}

type instance struct {
	client       *redis.Client
	store        *redisadapter.Store[string, product]
	synchronized *cache.Synchronized[string, product]
}

func newClient(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: environment(t, "E2E_REDIS_ADDRESS")})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	require.NoError(t, client.Ping(t.Context()).Err())
	return client
}

func environment(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is not set; run the suite with `just test-e2e`", name)
	}
	return value
}

// newInstance builds one process's cache over the namespace and event channel named after the
// test, so tests never see each other's keys.
func newInstance(t *testing.T, client *redis.Client, ttl time.Duration) *instance {
	t.Helper()
	store, err := redisadapter.New[string, product](&redisadapter.Options[string]{
		Client:       client,
		KeyCodec:     cache.StringKeyCodec{},
		KeyPrefix:    "e2e:" + t.Name(),
		TTL:          ttl,
		EventChannel: "e2e:" + t.Name() + ":events",
		// Short, so the failover test sees a missed heartbeat well within its deadline.
		HeartbeatInterval: 500 * time.Millisecond,
		Logger:            slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	require.NoError(t, err)
	synchronized, err := cache.NewSynchronized(&cache.SynchronizedOptions[string, product]{Backend: store})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupContext := context.WithoutCancel(t.Context())
		require.NoError(t, synchronized.Close(cleanupContext))
		store.Close()
		require.NoError(t, store.RemovePrefix(cleanupContext, ""))
	})
	require.NoError(t, store.AwaitSubscription(t.Context()))
	return &instance{client: client, store: store, synchronized: synchronized}
}

func requireEventually(t *testing.T, synchronized *cache.Synchronized[string, product], key string, want product) {
	t.Helper()
	require.Eventually(t, func() bool {
		value, err := synchronized.Get(t.Context(), key)
		return err == nil && value == want
	}, propagationDeadline, 10*time.Millisecond)
}

func TestAChangeOnOneInstanceReachesTheOthers(t *testing.T) {
	writer := newInstance(t, newClient(t), 0)
	reader := newInstance(t, newClient(t), 0)
	desk := product{Name: "desk", PriceCents: 24900}
	discountedDesk := product{Name: "desk", PriceCents: 19900}

	require.NoError(t, writer.synchronized.Set(t.Context(), "product:desk", desk))
	requireEventually(t, reader.synchronized, "product:desk", desk)

	require.NoError(t, writer.synchronized.Set(t.Context(), "product:desk", discountedDesk))
	requireEventually(t, reader.synchronized, "product:desk", discountedDesk)

	require.NoError(t, writer.synchronized.Remove(t.Context(), "product:desk"))
	require.Eventually(t, func() bool {
		_, err := reader.synchronized.Get(t.Context(), "product:desk")
		return errorIsNotFound(err)
	}, propagationDeadline, 10*time.Millisecond)
}

func TestRemovePrefixClearsMatchingKeysEverywhere(t *testing.T) {
	writer := newInstance(t, newClient(t), 0)
	reader := newInstance(t, newClient(t), 0)
	for _, key := range []string{"product:desk", "product:chair", "order:1"} {
		require.NoError(t, writer.synchronized.Set(t.Context(), key, product{Name: key}))
		requireEventually(t, reader.synchronized, key, product{Name: key})
	}

	require.NoError(t, writer.synchronized.RemovePrefix(t.Context(), "product:"))

	require.Eventually(t, func() bool {
		_, deskError := reader.synchronized.Get(t.Context(), "product:desk")
		_, chairError := reader.synchronized.Get(t.Context(), "product:chair")
		return errorIsNotFound(deskError) && errorIsNotFound(chairError)
	}, propagationDeadline, 10*time.Millisecond)
	order, err := reader.synchronized.Get(t.Context(), "order:1")
	require.NoError(t, err)
	require.Equal(t, product{Name: "order:1"}, order)
}

func TestPreloadCopiesTheSharedEntries(t *testing.T) {
	writer := newInstance(t, newClient(t), 0)
	for _, key := range []string{"product:desk", "product:chair"} {
		require.NoError(t, writer.synchronized.Set(t.Context(), key, product{Name: key}))
	}
	latecomer := newInstance(t, newClient(t), 0)

	require.NoError(t, latecomer.synchronized.Preload(t.Context()))

	namespace := "e2e:" + t.Name() + ":"
	// Bypassing the stores leaves the latecomer's local copies in place.
	require.NoError(t, writer.client.Del(t.Context(), namespace+"product:desk", namespace+"product:chair").Err())
	for _, key := range []string{"product:desk", "product:chair"} {
		value, err := latecomer.synchronized.Get(t.Context(), key)
		require.NoError(t, err)
		require.Equal(t, product{Name: key}, value)
	}
}

func TestEntriesExpireInRedis(t *testing.T) {
	writer := newInstance(t, newClient(t), time.Second)
	require.NoError(t, writer.store.Set(t.Context(), "product:desk", product{Name: "desk"}))

	remaining, err := writer.store.TTL(t.Context(), "product:desk")
	require.NoError(t, err)
	require.Greater(t, remaining, time.Duration(0))
	require.LessOrEqual(t, remaining, time.Second)

	require.Eventually(t, func() bool {
		_, err := writer.store.Get(t.Context(), "product:desk")
		return errorIsNotFound(err)
	}, propagationDeadline, 50*time.Millisecond)
}

func TestADroppedSubscriptionResetsTheLocalCopies(t *testing.T) {
	writer := newInstance(t, newClient(t), 0)
	reader := newInstance(t, newClient(t), 0)
	require.NoError(t, writer.synchronized.Set(t.Context(), "product:desk", product{Name: "desk"}))
	requireEventually(t, reader.synchronized, "product:desk", product{Name: "desk"})

	// A change without an event stands for one whose event was lost; only the reset after the
	// dropped subscription makes the reader see it.
	require.NoError(t, writer.client.Set(t.Context(), "e2e:"+t.Name()+":product:desk", mustEncode(t, product{Name: "changed"}), 0).Err())
	killSubscriptions(t, writer.client)

	requireEventually(t, reader.synchronized, "product:desk", product{Name: "changed"})
}

// killSubscriptions drops every connection that holds a subscription. CLIENT KILL TYPE pubsub
// would miss the RESP3 ones, which Redis counts as normal clients.
func killSubscriptions(t *testing.T, client *redis.Client) {
	t.Helper()
	clients, err := client.ClientList(t.Context()).Result()
	require.NoError(t, err)
	killed := 0
	for line := range strings.Lines(clients) {
		fields := strings.Fields(line)
		if slices.ContainsFunc(fields, holdsSubscription) {
			require.NoError(t, client.ClientKillByFilter(t.Context(), "ID", strings.TrimPrefix(fields[0], "id=")).Err())
			killed++
		}
	}
	require.Positive(t, killed)
}

func holdsSubscription(field string) bool {
	count, found := strings.CutPrefix(field, "sub=")
	return found && count != "0"
}
