//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const failoverDeadline = 30 * time.Second

// newFailoverClient connects through Sentinel to whichever server is the primary, the way a
// service in a Sentinel deployment does. It is a *redis.Client, so it fits every option of the
// library that takes one.
func newFailoverClient(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    environment(t, "E2E_SENTINEL_MASTER"),
		SentinelAddrs: []string{environment(t, "E2E_SENTINEL_ADDRESS")},
	})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	require.NoError(t, client.Ping(t.Context()).Err())
	return client
}

func newSentinel(t *testing.T) *redis.SentinelClient {
	t.Helper()
	sentinel := redis.NewSentinelClient(&redis.Options{Addr: environment(t, "E2E_SENTINEL_ADDRESS")})
	t.Cleanup(func() { require.NoError(t, sentinel.Close()) })
	return sentinel
}

func primaryAddress(t *testing.T, sentinel *redis.SentinelClient) string {
	t.Helper()
	address, err := sentinel.GetMasterAddrByName(t.Context(), environment(t, "E2E_SENTINEL_MASTER")).Result()
	require.NoError(t, err)
	return address[0] + ":" + address[1]
}

// waitForPromotableReplica returns once Sentinel has discovered a healthy replica, which it
// does from the primary's INFO only every few seconds; a failover before that fails.
func waitForPromotableReplica(t *testing.T, sentinel *redis.SentinelClient) {
	t.Helper()
	require.Eventually(t, func() bool {
		replicas, err := sentinel.Replicas(t.Context(), environment(t, "E2E_SENTINEL_MASTER")).Result()
		return err == nil && len(replicas) > 0 && replicas[0]["flags"] == "slave" && replicas[0]["master-link-status"] == "ok"
	}, failoverDeadline, 100*time.Millisecond)
}

func TestCachesAndElectionSurviveASentinelFailover(t *testing.T) {
	sentinel := newSentinel(t)
	waitForPromotableReplica(t, sentinel)
	writer := newInstance(t, newFailoverClient(t), 0)
	reader := newInstance(t, newFailoverClient(t), 0)
	elected := make(chan string, 4)
	election := newElection(t, newFailoverClient(t), "only", elected)
	require.NoError(t, election.Start(t.Context()))
	require.Equal(t, "only", <-elected)
	tokenBeforeFailover := election.FencingToken()

	require.NoError(t, writer.synchronized.Set(t.Context(), "product:desk", product{Name: "desk"}))
	requireEventually(t, reader.synchronized, "product:desk", product{Name: "desk"})
	// A change without an event: only the reset after the failover makes the reader see it.
	redisKey := "e2e:" + t.Name() + ":product:desk"
	require.NoError(t, writer.client.Set(t.Context(), redisKey, mustEncode(t, product{Name: "changed"}), 0).Err())
	require.NoError(t, writer.client.Wait(t.Context(), 1, failoverDeadline).Err())
	primaryBeforeFailover := primaryAddress(t, sentinel)

	require.NoError(t, sentinel.Failover(t.Context(), environment(t, "E2E_SENTINEL_MASTER")).Err())

	require.Eventually(t, func() bool {
		return primaryAddress(t, sentinel) != primaryBeforeFailover
	}, failoverDeadline, 100*time.Millisecond)
	require.Eventually(t, func() bool {
		value, err := reader.synchronized.Get(t.Context(), "product:desk")
		return err == nil && value == product{Name: "changed"}
	}, failoverDeadline, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		return writer.synchronized.Set(t.Context(), "product:chair", product{Name: "chair"}) == nil
	}, failoverDeadline, 100*time.Millisecond)
	requireEventually(t, reader.synchronized, "product:chair", product{Name: "chair"})

	require.Eventually(t, func() bool {
		return election.IsLeader() && election.FencingToken() >= tokenBeforeFailover
	}, failoverDeadline, 100*time.Millisecond)
}
