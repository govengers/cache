// Package leader elects one leader among processes sharing a Redis instance, with a lease the
// leader renews and a fencing token that grows with every new leadership.
package leader

import (
	"cmp"
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/govengers/cache/internal/recovery"
	"github.com/govengers/cache/internal/subscription"
	"github.com/redis/go-redis/v9"
)

// maximumRetryInterval keeps RetryInterval plus its jitter far from overflowing.
const maximumRetryInterval = 24 * time.Hour

// ErrAlreadyStarted is returned by Start on an election that was started before.
var ErrAlreadyStarted = errors.New("leader: election already started")

// Options configures New.
type Options struct {
	// Client is the Redis connection: redis.NewClient for one server, redis.NewFailoverClient
	// behind Sentinel, where a failover can lose a fresh lock with its epoch (replication is
	// asynchronous). Redis Cluster is not supported. The caller owns it and closes it after Stop.
	// Required.
	Client *redis.Client
	// LockKey is the Redis key of the lock; the fencing token counter is stored at
	// "<LockKey>:epoch". Required.
	LockKey string
	// InstanceID identifies this process in the lock value. Empty: a random ID.
	InstanceID string
	// LeaseDuration is how long the lock outlives the leader's last renewal, in whole
	// milliseconds (Redis's precision; anything finer is cut off). A crashed leader's lock
	// expires after at most this long; a follower takes it at its next attempt. At least a
	// millisecond. Required.
	LeaseDuration time.Duration
	// RenewInterval is how often the leader renews the lease. A leader steps down when no
	// renewal has succeeded by one RenewInterval before its lease may expire, so it must be
	// shorter than half of LeaseDuration. Zero: a third of LeaseDuration.
	RenewInterval time.Duration
	// RetryInterval is how often a follower tries to take the lock, plus up to a quarter of it as
	// jitter. At most a day. Zero: half of LeaseDuration.
	RetryInterval time.Duration
	// OnElected runs in its own goroutine when this process becomes leader, with the fencing
	// token of that leadership. Its context is cancelled when the leadership ends; it must
	// return then, because the election does not campaign again, and Stop does not return,
	// before it did. It must not call Stop. Optional.
	OnElected func(ctx context.Context, fencingToken int64)
	// OnDemoted runs when a leadership ends, after the context of OnElected was cancelled: on
	// the election goroutine, or in Stop. It must not block or call Stop. Optional.
	OnDemoted func()
	// EventChannel is a Pub/Sub channel on which a resigning leader announces itself, so
	// followers take over at once instead of at their next attempt. Each follower keeps its
	// subscription alive with a heartbeat (one PUBLISH and one INFO) every RetryInterval.
	// Empty: no announcements.
	EventChannel string
	// Logger receives Redis failures the election recovers from and panics in the callbacks.
	// Nil: nothing is logged.
	Logger *slog.Logger
}

// Election takes part in one leader election. It is safe for concurrent use.
type Election struct {
	options Options
	logger  *slog.Logger
	wake    chan struct{}
	running sync.WaitGroup
	// callSlot admits one Redis call of the election at a time: a call abandoned at its
	// deadline holds it until go-redis gives up on the call, so abandoned calls cannot pile up.
	callSlot chan struct{}
	// releaseSlot does the same for background releases of the lock.
	releaseSlot chan struct{}

	mutex   sync.Mutex
	started bool
	stopped bool
	stop    context.CancelFunc
	current *leadership // nil while following
}

// leadership is one term of this process as leader.
type leadership struct {
	fencingToken   int64
	lockValue      string
	leaseStartedAt time.Time // when the last successful acquisition or renewal was sent
	cancel         context.CancelFunc
	callbackDone   chan struct{} // closed when OnElected returned
}

// New returns an Election that has not started yet, or an error if options are
// incomplete or inconsistent.
func New(options *Options) (*Election, error) {
	resolved, err := resolve(options)
	if err != nil {
		return nil, err
	}
	return &Election{
		options:     resolved,
		logger:      cmp.Or(resolved.Logger, slog.New(slog.DiscardHandler)),
		wake:        make(chan struct{}, 1),
		callSlot:    make(chan struct{}, 1),
		releaseSlot: make(chan struct{}, 1),
	}, nil
}

func resolve(options *Options) (Options, error) {
	if options == nil || options.Client == nil || options.LockKey == "" {
		return Options{}, errors.New("leader: Options.Client and LockKey are required")
	}
	resolved := *options
	resolved.LeaseDuration = resolved.LeaseDuration.Truncate(time.Millisecond)
	resolved.InstanceID = cmp.Or(resolved.InstanceID, cryptorand.Text())
	resolved.RenewInterval = cmp.Or(resolved.RenewInterval, resolved.LeaseDuration/3)
	resolved.RetryInterval = cmp.Or(resolved.RetryInterval, resolved.LeaseDuration/2)
	if resolved.LeaseDuration < time.Millisecond {
		return Options{}, fmt.Errorf("leader: LeaseDuration %s is shorter than a millisecond", options.LeaseDuration)
	}
	if resolved.RenewInterval <= 0 || resolved.RenewInterval >= resolved.LeaseDuration-resolved.RenewInterval {
		return Options{}, fmt.Errorf("leader: RenewInterval %s must be positive and shorter than half of LeaseDuration %s", resolved.RenewInterval, resolved.LeaseDuration)
	}
	if resolved.RetryInterval <= 0 || resolved.RetryInterval > maximumRetryInterval {
		return Options{}, fmt.Errorf("leader: RetryInterval %s must be positive and at most %s", resolved.RetryInterval, maximumRetryInterval)
	}
	return resolved, nil
}

// IsLeader reports whether this process holds the lock as far as it knows. A leader steps down
// one RenewInterval before its lease may expire unless a renewal succeeded, even while Redis
// hangs, so clocks that drift by less than that cannot make two processes lead at once. A
// paused process (GC, a frozen VM) can still overrun its lease: guard writes that must never
// overlap with the fencing token.
func (e *Election) IsLeader() bool {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	return e.current != nil
}

// FencingToken returns the token of the current leadership, which is larger than that of every
// earlier leadership, or zero when this process is not the leader. Storage that rejects writes
// carrying a smaller token than it has seen keeps a stale leader from overwriting a newer one.
// Work started by OnElected should use the token it was given instead, which cannot change
// under it.
func (e *Election) FencingToken() int64 {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	if e.current == nil {
		return 0
	}
	return e.current.fencingToken
}

// Start runs the election in the background until ctx ends or Stop is called. When ctx ends, a
// leader steps down and releases the lock; Stop is still needed to wait for the goroutines. A
// panic in the election itself (not in a callback, which is recovered on its own) is logged and
// ends the election the same way; it does not campaign again. It returns ErrAlreadyStarted on a
// second call.
func (e *Election) Start(ctx context.Context) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	if e.started {
		return ErrAlreadyStarted
	}
	e.started = true
	electionContext, stop := context.WithCancel(ctx)
	e.stop = stop

	if e.options.EventChannel != "" {
		e.running.Go(func() {
			recovery.Run(e.logger, "leader: receiving resignations", func() {
				subscription.Keep(electionContext, &subscription.Parameters{
					Client:  e.options.Client,
					Channel: e.options.EventChannel,
					Logger:  e.logger,
					// A follower that misses resignations still campaigns every RetryInterval.
					HeartbeatInterval: e.options.RetryInterval,
					OnMessage:         e.wakeFollower,
				})
			})
		})
	}
	e.running.Go(func() {
		recovery.Run(e.logger, "leader: election", func() { e.run(electionContext) })
		// Also after a panic in run, so a leadership never outlives its election.
		recovery.Run(e.logger, "leader: ending the election", func() { e.resignUnlessStopping(electionContext) })
	})
	return nil
}

// Stop ends the election and waits for its goroutines and OnElected to return. A leader then
// steps down, releases the lock and announces its resignation. Stop returns the error of
// releasing the lock; the lock then expires after LeaseDuration. ctx bounds the release only if
// the client sets ContextTimeoutEnabled; otherwise the client's own timeouts do. Calling it
// again, or before Start, does nothing.
func (e *Election) Stop(ctx context.Context) error {
	e.mutex.Lock()
	if !e.started || e.stopped {
		e.mutex.Unlock()
		return nil
	}
	e.stopped = true
	e.mutex.Unlock()

	e.stop()
	// Step down before waiting: an abandoned Redis call can take until the client's timeouts,
	// and the process must not claim the leadership meanwhile.
	// promote refuses once the election context ended, so no term begins after this demote.
	ended := e.demote()
	e.running.Wait()
	if ended == nil {
		return nil
	}
	return e.resign(ctx, ended.lockValue)
}

func (e *Election) run(ctx context.Context) {
	for {
		current := e.campaign(ctx)
		if current == nil {
			return
		}
		e.holdLeadership(ctx, current)
		if !e.awaitCallback(ctx, current) {
			return
		}
	}
}

// campaign tries to take the lock until it succeeds or ctx ends.
func (e *Election) campaign(ctx context.Context) *leadership {
	for {
		if current := e.tryAcquire(ctx); current != nil {
			return current
		}
		if !e.waitForRetry(ctx) {
			return nil
		}
	}
}

func (e *Election) waitForRetry(ctx context.Context) bool {
	timer := time.NewTimer(e.options.RetryInterval + rand.N(e.options.RetryInterval/4+1)) //nolint:gosec // G404: jitter does not need a cryptographic RNG
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	case <-e.wake:
		// Every follower wakes on the same resignation; a short random delay spreads their
		// attempts.
		return sleep(ctx, rand.N(e.options.RetryInterval/10+1)) //nolint:gosec // G404: jitter does not need a cryptographic RNG
	}
}

// holdLeadership renews the lease until ctx ends or the leadership is lost: at once when another
// process holds the lock, or when no renewal succeeded until one RenewInterval before the lease
// may expire. A lease runs from the moment its request was sent, the earliest Redis can start it.
func (e *Election) holdLeadership(ctx context.Context, current *leadership) {
	ticker := time.NewTicker(e.options.RenewInterval)
	defer ticker.Stop()
	leadUntil := e.leadUntil(current.leaseStartedAt)
	deadline := time.NewTimer(time.Until(leadUntil))
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			e.logger.Warn("leader: no renewal succeeded in time, stepping down before the lease may expire", "lock", e.options.LockKey)
			// The lock may still hold this term's value; releasing it lets a follower take over
			// at once instead of after the lease. Stop releases on its own if it demoted first.
			if ended := e.demote(); ended != nil {
				e.resignInBackground(ended.lockValue)
			}
			return
		case <-ticker.C:
		}
		attemptedAt := time.Now()
		held, err := e.renew(ctx, current.lockValue, leadUntil)
		switch {
		case ctx.Err() != nil:
			return
		case err == nil && held:
			leadUntil = e.leadUntil(attemptedAt)
			deadline.Reset(time.Until(leadUntil))
		case err == nil:
			e.logger.Warn("leader: another process holds the lock, stepping down", "lock", e.options.LockKey)
			e.demote()
			return
		default:
			e.logger.Warn("leader: renewing the lease failed", "lock", e.options.LockKey, "error", err)
		}
	}
}

// leadUntil is when a leader whose lease started at leaseStartedAt steps down unless a renewal
// succeeded.
func (e *Election) leadUntil(leaseStartedAt time.Time) time.Time {
	return leaseStartedAt.Add(e.options.LeaseDuration - e.options.RenewInterval)
}

func (e *Election) promote(ctx context.Context, fencingToken int64, lockValue string, leaseStartedAt time.Time) *leadership {
	leadershipContext, cancel := context.WithCancel(ctx)
	current := &leadership{
		fencingToken:   fencingToken,
		lockValue:      lockValue,
		leaseStartedAt: leaseStartedAt,
		cancel:         cancel,
		callbackDone:   make(chan struct{}),
	}
	e.mutex.Lock()
	if ctx.Err() != nil {
		// Stop or the end of Start's context came first; the lock goes back at once.
		e.mutex.Unlock()
		cancel()
		e.resignInBackground(lockValue)
		return nil
	}
	e.current = current
	e.mutex.Unlock()

	if e.options.OnElected == nil {
		close(current.callbackDone)
		return current
	}
	e.running.Go(func() {
		defer close(current.callbackDone)
		recovery.Run(e.logger, "leader: OnElected", func() { e.options.OnElected(leadershipContext, fencingToken) })
	})
	return current
}

// demote ends the current leadership, if any, and returns it.
func (e *Election) demote() *leadership {
	e.mutex.Lock()
	ended := e.current
	e.current = nil
	e.mutex.Unlock()
	if ended == nil {
		return nil
	}
	ended.cancel()
	if e.options.OnDemoted != nil {
		recovery.Run(e.logger, "leader: OnDemoted", e.options.OnDemoted)
	}
	return ended
}

// awaitCallback waits until the OnElected of an ended leadership returned, so two of them never
// run at once, and reports false if ctx ended first.
func (e *Election) awaitCallback(ctx context.Context, ended *leadership) bool {
	select {
	case <-ended.callbackDone:
		return true
	case <-ctx.Done():
		return false
	}
}

// resignUnlessStopping ends a leadership whose election ended because the context given to Start
// was cancelled, or because the election panicked. Stop resigns on its own, so it can return the
// error.
func (e *Election) resignUnlessStopping(ctx context.Context) {
	e.mutex.Lock()
	stopping := e.stopped
	e.mutex.Unlock()
	if stopping {
		return
	}
	ended := e.demote()
	if ended == nil {
		return
	}
	resignContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.options.LeaseDuration)
	defer cancel()
	if err := e.resign(resignContext, ended.lockValue); err != nil {
		e.logger.Warn("leader: resigning failed, the lock expires with its lease", "lock", e.options.LockKey, "error", err)
	}
}

// resignInBackground releases lockValue if the lock still holds it, without waiting for Redis.
// One release runs at a time: while one hangs, later ones are skipped and their locks expire with
// their lease, so hanging releases cannot pile up.
func (e *Election) resignInBackground(lockValue string) {
	select {
	case e.releaseSlot <- struct{}{}:
	default:
		e.logger.Warn("leader: a release is still in flight, the lock expires with its lease", "lock", e.options.LockKey)
		return
	}
	e.running.Go(func() {
		defer func() { <-e.releaseSlot }()
		recovery.Run(e.logger, "leader: resigning", func() {
			resignContext, cancel := context.WithTimeout(context.Background(), e.options.LeaseDuration)
			defer cancel()
			if err := e.resign(resignContext, lockValue); err != nil {
				e.logger.Warn("leader: resigning failed, the lock expires with its lease", "lock", e.options.LockKey, "error", err)
			}
		})
	})
}

func (e *Election) wakeFollower(string) {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}
