package leader

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

const (
	testLockKey      = "test:leader"
	testEventChannel = "test:election"
	testLease        = 300 * time.Millisecond
	waitTimeout      = 2 * time.Second
	pollInterval     = 5 * time.Millisecond
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// newClient returns a client of miniRedis that is closed when the test ends.
func newClient(t *testing.T, miniRedis *miniredis.Miniredis) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: miniRedis.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client
}

func testOptions(client *redis.Client) *ElectionOptions {
	return &ElectionOptions{Client: client, LockKey: testLockKey, LeaseDuration: testLease}
}

// newElection returns an election that is stopped when the test ends, before its client is
// closed.
func newElection(t *testing.T, options *ElectionOptions) *Election {
	t.Helper()
	election, err := NewElection(options)
	require.NoError(t, err)
	t.Cleanup(func() { _ = election.Stop(context.Background()) })
	return election
}

func startElection(t *testing.T, options *ElectionOptions) *Election {
	t.Helper()
	election := newElection(t, options)
	require.NoError(t, election.Start(context.Background()))
	return election
}

func waitForLeadership(t *testing.T, election *Election) {
	t.Helper()
	require.Eventually(t, election.IsLeader, waitTimeout, pollInterval)
}

func receive[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(waitTimeout):
		require.FailNow(t, "timed out waiting for a value on the channel")
		var zero T
		return zero
	}
}

// logBuffer collects the log output the election writes from its goroutines.
type logBuffer struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (b *logBuffer) Write(data []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(data)
}

func (b *logBuffer) contains(text string) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return strings.Contains(b.buffer.String(), text)
}

func (b *logBuffer) logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(b, nil))
}

// answerRecorder remembers when the last command started that Redis answered without error.
type answerRecorder struct {
	mutex      sync.Mutex
	lastAnswer time.Time
}

func (r *answerRecorder) DialHook(next redis.DialHook) redis.DialHook { return next }

func (r *answerRecorder) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (r *answerRecorder) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		started := time.Now()
		err := next(ctx, command)
		if err == nil {
			r.mutex.Lock()
			r.lastAnswer = started
			r.mutex.Unlock()
		}
		return err
	}
}

func (r *answerRecorder) lastAnswerStarted() time.Time {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.lastAnswer
}

func TestElectionSingleInstanceLeads(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	elected := make(chan struct{}, 1)
	options := testOptions(newClient(t, miniRedis))
	options.OnElected = func(context.Context) { elected <- struct{}{} }
	election := startElection(t, options)

	receive(t, elected)
	require.True(t, election.IsLeader())
	require.Positive(t, election.FencingToken())
	lockValueInRedis, err := miniRedis.Get(testLockKey)
	require.NoError(t, err)
	require.Equal(t, lockValue(election.options.InstanceID, election.FencingToken()), lockValueInRedis)

	require.NoError(t, election.Stop(context.Background()))
	require.False(t, election.IsLeader())
	require.Zero(t, election.FencingToken())
}

func TestElectionGracefulResignation(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	var demotions atomic.Int32
	options := testOptions(newClient(t, miniRedis))
	options.OnDemoted = func() { demotions.Add(1) }
	election := startElection(t, options)
	waitForLeadership(t, election)

	require.NoError(t, election.Stop(context.Background()))

	require.Equal(t, int32(1), demotions.Load())
	require.False(t, miniRedis.Exists(testLockKey))
}

func TestElectionTwoInstancesElectOneLeader(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	client := newClient(t, miniRedis)
	firstOptions := testOptions(client)
	firstOptions.InstanceID = "first"
	secondOptions := testOptions(client)
	secondOptions.InstanceID = "second"
	first := startElection(t, firstOptions)
	second := startElection(t, secondOptions)

	require.Eventually(t, func() bool { return first.IsLeader() || second.IsLeader() }, waitTimeout, pollInterval)
	require.Never(t, func() bool { return first.IsLeader() && second.IsLeader() }, testLease, pollInterval)

	incumbent, follower := first, second
	if second.IsLeader() {
		incumbent, follower = second, first
	}
	incumbentToken := incumbent.FencingToken()
	require.NoError(t, incumbent.Stop(context.Background()))

	waitForLeadership(t, follower)
	require.Greater(t, follower.FencingToken(), incumbentToken)
}

func TestElectionFencingTokenGrowsWithEveryLeadership(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	client := newClient(t, miniRedis)

	var tokens []int64
	for range 5 {
		election := startElection(t, testOptions(client))
		waitForLeadership(t, election)
		tokens = append(tokens, election.FencingToken())
		require.NoError(t, election.Stop(context.Background()))
	}

	for index := 1; index < len(tokens); index++ {
		require.Greater(t, tokens[index], tokens[index-1], "tokens %v", tokens)
	}
}

func TestElectionFollowerTakesOverQuicklyOnResignation(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	client := newClient(t, miniRedis)
	const retryInterval = 10 * time.Second
	optionsFor := func(instanceID string) *ElectionOptions {
		return &ElectionOptions{
			Client:        client,
			LockKey:       testLockKey,
			InstanceID:    instanceID,
			LeaseDuration: 30 * time.Second,
			RetryInterval: retryInterval,
			EventChannel:  testEventChannel,
		}
	}
	incumbent := startElection(t, optionsFor("incumbent"))
	waitForLeadership(t, incumbent)
	follower := startElection(t, optionsFor("follower"))
	require.Eventually(t, func() bool {
		return miniRedis.PubSubNumSub(testEventChannel)[testEventChannel] == 2
	}, waitTimeout, pollInterval)

	require.NoError(t, incumbent.Stop(context.Background()))

	// Without the announcement the follower would retry only after the retry interval.
	require.Eventually(t, follower.IsLeader, retryInterval/4, pollInterval)
}

func TestElectionFollowerTakesOverAfterLeaseExpiry(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	// A crashed leader left its lock behind.
	require.NoError(t, miniRedis.Set(testLockKey, "crashed:1"))
	miniRedis.SetTTL(testLockKey, testLease)
	options := testOptions(newClient(t, miniRedis))
	options.RetryInterval = 20 * time.Millisecond
	election := startElection(t, options)

	require.Never(t, election.IsLeader, 5*options.RetryInterval, pollInterval)
	miniRedis.FastForward(testLease)

	waitForLeadership(t, election)
}

func TestElectionRenewsLease(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	election := startElection(t, testOptions(newClient(t, miniRedis)))
	waitForLeadership(t, election)

	miniRedis.FastForward(testLease / 2)

	require.Eventually(t, func() bool { return miniRedis.TTL(testLockKey) == testLease }, waitTimeout, pollInterval)
	require.True(t, election.IsLeader())
}

func TestElectionDemotesAtOnceWhenAnotherInstanceHoldsTheLock(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	demoted := make(chan struct{}, 1)
	options := testOptions(newClient(t, miniRedis))
	// The lease is longer than receive waits, so the demotion cannot be due to its expiry.
	options.LeaseDuration = 5 * time.Second
	options.RenewInterval = 20 * time.Millisecond
	options.OnDemoted = func() { demoted <- struct{}{} }
	election := startElection(t, options)
	waitForLeadership(t, election)

	require.NoError(t, miniRedis.Set(testLockKey, "other:99"))

	receive(t, demoted)
	require.False(t, election.IsLeader())
	require.Zero(t, election.FencingToken())
	require.NoError(t, election.Stop(context.Background()))
	lockValueInRedis, err := miniRedis.Get(testLockKey)
	require.NoError(t, err)
	require.Equal(t, "other:99", lockValueInRedis)
}

func TestElectionResignsBeforeLeaseExpiresWhenRedisIsUnreachable(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	logs := &logBuffer{}
	recorder := &answerRecorder{}
	client := newClient(t, miniRedis)
	client.AddHook(recorder)
	demotedAt := make(chan time.Time, 1)
	options := testOptions(client)
	options.LeaseDuration = 600 * time.Millisecond
	options.RenewInterval = 100 * time.Millisecond
	options.RetryInterval = 10 * time.Second
	options.Logger = logs.logger()
	options.OnDemoted = func() { demotedAt <- time.Now() }
	election := startElection(t, options)
	waitForLeadership(t, election)

	miniRedis.Close()

	demotion := receive(t, demotedAt)
	// Redis cannot have renewed the lease before the last answered command started.
	earliestExpiry := recorder.lastAnswerStarted().Add(options.LeaseDuration)
	require.True(t, demotion.Before(earliestExpiry), "demoted %s after the lease may have expired", demotion.Sub(earliestExpiry))
	require.False(t, election.IsLeader())
	require.True(t, logs.contains("resigning before it expires"))
}

func TestElectionLogsFailedAcquisitionAndRetries(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	miniRedis.SetError("ERR injected failure")
	logs := &logBuffer{}
	options := testOptions(newClient(t, miniRedis))
	options.RetryInterval = 20 * time.Millisecond
	options.Logger = logs.logger()
	election := startElection(t, options)

	require.Eventually(t, func() bool { return logs.contains("leader: acquiring the lock failed") }, waitTimeout, pollInterval)
	require.False(t, election.IsLeader())

	miniRedis.SetError("")
	waitForLeadership(t, election)
}

func TestElectionRecoversPanicsInCallbacks(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	logs := &logBuffer{}
	options := testOptions(newClient(t, miniRedis))
	options.Logger = logs.logger()
	options.OnElected = func(context.Context) { panic("elected failure") }
	options.OnDemoted = func() { panic("demoted failure") }
	election := startElection(t, options)
	waitForLeadership(t, election)

	require.Eventually(t, func() bool { return logs.contains("leader: OnElected panicked") }, waitTimeout, pollInterval)
	require.True(t, election.IsLeader())

	require.NoError(t, election.Stop(context.Background()))
	require.True(t, logs.contains("leader: OnDemoted panicked"))
	require.False(t, miniRedis.Exists(testLockKey))
}

func TestElectionCancelsOnElectedContextAndWaitsForIt(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	leadershipContexts := make(chan context.Context, 1)
	var returned atomic.Bool
	options := testOptions(newClient(t, miniRedis))
	options.OnElected = func(ctx context.Context) {
		leadershipContexts <- ctx
		<-ctx.Done()
		returned.Store(true)
	}
	election := startElection(t, options)

	leadershipContext := receive(t, leadershipContexts)
	require.NoError(t, leadershipContext.Err())

	require.NoError(t, election.Stop(context.Background()))
	require.ErrorIs(t, leadershipContext.Err(), context.Canceled)
	require.True(t, returned.Load())
}

func TestElectionStopReturnsOpaqueErrorWhenReleaseFails(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	options := testOptions(newClient(t, miniRedis))
	options.LeaseDuration = 10 * time.Second
	election := startElection(t, options)
	waitForLeadership(t, election)

	miniRedis.SetError("ERR injected failure")
	err := election.Stop(context.Background())

	require.ErrorContains(t, err, "leader: releasing lock "+testLockKey)
	require.ErrorContains(t, err, "injected failure")
	var redisError redis.Error
	require.False(t, errors.As(err, &redisError), "the error exposes go-redis type %T", redisError)
	require.False(t, election.IsLeader())
}

func TestElectionStopReportsCancelledContext(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	election := startElection(t, testOptions(newClient(t, miniRedis)))
	waitForLeadership(t, election)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := election.Stop(ctx)

	require.ErrorIs(t, err, context.Canceled)
}

func TestElectionStopIsIdempotent(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	var demotions atomic.Int32
	options := testOptions(newClient(t, miniRedis))
	options.OnDemoted = func() { demotions.Add(1) }
	election := startElection(t, options)
	waitForLeadership(t, election)

	require.NoError(t, election.Stop(context.Background()))
	require.NoError(t, election.Stop(context.Background()))
	require.Equal(t, int32(1), demotions.Load())
}

func TestElectionStopBeforeStartDoesNothing(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	election := newElection(t, testOptions(newClient(t, miniRedis)))

	require.NoError(t, election.Stop(context.Background()))
	require.False(t, election.IsLeader())
}

func TestElectionStartTwiceFails(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	election := startElection(t, testOptions(newClient(t, miniRedis)))

	require.ErrorIs(t, election.Start(context.Background()), ErrAlreadyStarted)
	require.NoError(t, election.Stop(context.Background()))
	require.ErrorIs(t, election.Start(context.Background()), ErrAlreadyStarted)
}

func TestElectionParentContextCancellationEndsLeadership(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	leadershipContexts := make(chan context.Context, 1)
	demoted := make(chan struct{}, 1)
	options := testOptions(newClient(t, miniRedis))
	options.OnElected = func(ctx context.Context) { leadershipContexts <- ctx }
	options.OnDemoted = func() { demoted <- struct{}{} }
	election := newElection(t, options)
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, election.Start(ctx))
	leadershipContext := receive(t, leadershipContexts)

	cancel()

	require.Eventually(t, func() bool { return leadershipContext.Err() != nil }, waitTimeout, pollInterval)
	// The election no longer renews the lease, so it must not keep claiming the leadership.
	require.Eventually(t, func() bool { return !election.IsLeader() }, waitTimeout, pollInterval)
	receive(t, demoted)
	require.Eventually(t, func() bool { return !miniRedis.Exists(testLockKey) }, waitTimeout, pollInterval)
	require.NoError(t, election.Stop(context.Background()))
}

func TestElectionIsLeaderIsSafeForConcurrentUse(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	election := newElection(t, testOptions(newClient(t, miniRedis)))
	done := make(chan struct{})
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
					election.IsLeader()
					election.FencingToken()
				}
			}
		})
	}

	require.NoError(t, election.Start(context.Background()))
	waitForLeadership(t, election)
	require.NoError(t, election.Stop(context.Background()))
	close(done)
	readers.Wait()
}

func TestNewElectionRejectsInvalidOptions(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:0"})
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	testCases := []struct {
		name    string
		options *ElectionOptions
	}{
		{name: "nil options", options: nil},
		{name: "nil client", options: &ElectionOptions{LockKey: testLockKey, LeaseDuration: time.Second}},
		{name: "empty lock key", options: &ElectionOptions{Client: client, LeaseDuration: time.Second}},
		{name: "zero lease", options: &ElectionOptions{Client: client, LockKey: testLockKey}},
		{name: "negative lease", options: &ElectionOptions{Client: client, LockKey: testLockKey, LeaseDuration: -time.Second}},
		{name: "lease below a millisecond", options: &ElectionOptions{Client: client, LockKey: testLockKey, LeaseDuration: time.Millisecond - 1}},
		{name: "negative renew interval", options: &ElectionOptions{Client: client, LockKey: testLockKey, LeaseDuration: time.Second, RenewInterval: -time.Millisecond}},
		{name: "renew interval half of lease", options: &ElectionOptions{Client: client, LockKey: testLockKey, LeaseDuration: time.Second, RenewInterval: time.Second / 2}},
		{name: "renew interval equal to lease", options: &ElectionOptions{Client: client, LockKey: testLockKey, LeaseDuration: time.Second, RenewInterval: time.Second}},
		{name: "renew interval longer than lease", options: &ElectionOptions{Client: client, LockKey: testLockKey, LeaseDuration: time.Second, RenewInterval: 2 * time.Second}},
		{name: "negative retry interval", options: &ElectionOptions{Client: client, LockKey: testLockKey, LeaseDuration: time.Second, RetryInterval: -time.Millisecond}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			election, err := NewElection(testCase.options)

			require.Error(t, err)
			require.Nil(t, election)
		})
	}
}

func TestNewElectionAppliesDefaults(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:0"})
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	testCases := []struct {
		name                  string
		options               ElectionOptions
		expectedRenewInterval time.Duration
		expectedRetryInterval time.Duration
	}{
		{
			name:                  "derived from the lease",
			options:               ElectionOptions{Client: client, LockKey: testLockKey, LeaseDuration: 9 * time.Second},
			expectedRenewInterval: 3 * time.Second,
			expectedRetryInterval: 4500 * time.Millisecond,
		},
		{
			name:                  "explicit values kept",
			options:               ElectionOptions{Client: client, LockKey: testLockKey, LeaseDuration: 9 * time.Second, RenewInterval: time.Second, RetryInterval: 2 * time.Second},
			expectedRenewInterval: time.Second,
			expectedRetryInterval: 2 * time.Second,
		},
		{
			name:                  "shortest lease",
			options:               ElectionOptions{Client: client, LockKey: testLockKey, LeaseDuration: time.Millisecond},
			expectedRenewInterval: time.Millisecond / 3,
			expectedRetryInterval: time.Millisecond / 2,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			election, err := NewElection(&testCase.options)

			require.NoError(t, err)
			require.Equal(t, testCase.expectedRenewInterval, election.options.RenewInterval)
			require.Equal(t, testCase.expectedRetryInterval, election.options.RetryInterval)
			require.NotEmpty(t, election.options.InstanceID)
		})
	}
}

func TestNewElectionGeneratesDistinctInstanceIDs(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:0"})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	options := &ElectionOptions{Client: client, LockKey: testLockKey, LeaseDuration: time.Second}

	first, err := NewElection(options)
	require.NoError(t, err)
	second, err := NewElection(options)
	require.NoError(t, err)

	require.NotEqual(t, first.options.InstanceID, second.options.InstanceID)
	require.Empty(t, options.InstanceID, "NewElection must not modify the caller's options")
}
