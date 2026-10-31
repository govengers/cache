//go:build e2e

package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/govengers/cache"
	"github.com/govengers/cache/leader"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func errorIsNotFound(err error) bool {
	return errors.Is(err, cache.ErrNotFound)
}

func mustEncode(t *testing.T, value product) []byte {
	t.Helper()
	encoded, err := msgpack.Marshal(value)
	require.NoError(t, err)
	return encoded
}

func newElection(t *testing.T, client *redis.Client, instanceID string, elected chan<- string) *leader.Election {
	t.Helper()
	election, err := leader.New(&leader.Options{
		Client:        client,
		LockKey:       "e2e:" + t.Name() + ":lock",
		InstanceID:    instanceID,
		LeaseDuration: 10 * time.Second,
		RetryInterval: 10 * time.Second, // a takeover within the deadline needs the resignation event
		EventChannel:  "e2e:" + t.Name() + ":resignations",
		OnElected:     func(context.Context, int64) { elected <- instanceID },
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, election.Stop(context.WithoutCancel(t.Context()))) })
	return election
}

func TestAFollowerTakesOverAsSoonAsTheLeaderResigns(t *testing.T) {
	elected := make(chan string, 2)
	first := newElection(t, newClient(t), "first", elected)
	second := newElection(t, newClient(t), "second", elected)

	require.NoError(t, first.Start(t.Context()))
	require.Equal(t, "first", <-elected)
	firstToken := first.FencingToken()
	require.NoError(t, second.Start(t.Context()))
	// Both elections listen for resignations: the second must hear the first's.
	waitForSubscribers(t, newClient(t), "e2e:"+t.Name()+":resignations", 2)
	require.False(t, second.IsLeader())

	require.NoError(t, first.Stop(t.Context()))

	select {
	case instanceID := <-elected:
		require.Equal(t, "second", instanceID)
	case <-time.After(propagationDeadline):
		t.Fatal("the follower did not take over after the resignation")
	}
	require.False(t, first.IsLeader())
	require.Greater(t, second.FencingToken(), firstToken)
}

// waitForSubscribers returns once count subscriptions listen on channel, so nothing the test
// publishes there is lost.
func waitForSubscribers(t *testing.T, client *redis.Client, channel string, count int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		subscribers, err := client.PubSubNumSub(t.Context(), channel).Result()
		return err == nil && subscribers[channel] >= count
	}, propagationDeadline, 10*time.Millisecond)
}
