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
	"strconv"
	"sync"
	"time"

	"github.com/govengers/cache/internal/recovery"
	"github.com/govengers/cache/internal/subscription"
	"github.com/redis/go-redis/v9"
)

// ErrAlreadyStarted is returned by Start on an election that was started before.
var ErrAlreadyStarted = errors.New("leader: election already started")

// ElectionOptions configures NewElection.
type ElectionOptions struct {
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
	// LeaseDuration is how long the lock outlives the leader's last renewal; a crashed leader is
	// replaced after at most this long. At least a millisecond. Required.
	LeaseDuration time.Duration
	// RenewInterval is how often the leader renews the lease. A leader whose renewals have not
	// succeeded by one RenewInterval before the lease may expire steps down, so it must be
	// shorter than half of LeaseDuration. Zero: a third of LeaseDuration.
	RenewInterval time.Duration
	// RetryInterval is how often a follower tries to take the lock, plus up to a quarter of it as
	// jitter. Zero: half of LeaseDuration.
	RetryInterval time.Duration
	// OnElected runs in its own goroutine when this process becomes leader. Its context is
	// cancelled when the leadership ends; it must return then, because Stop waits for it.
	// Optional.
	OnElected func(ctx context.Context)
	// OnDemoted runs on the election goroutine when the leadership ends, after the context of
	// OnElected was cancelled. It must not block. Optional.
	OnDemoted func()
	// EventChannel is a Pub/Sub channel on which a resigning leader announces itself, so
	// followers take over at once instead of after the lease expired. Empty: no announcements.
	EventChannel string
	// Logger receives Redis failures the election recovers from and panics in the callbacks.
	// Nil: nothing is logged.
	Logger *slog.Logger
}

// Election takes part in one leader election. It is safe for concurrent use.
type Election struct {
	options ElectionOptions
	logger  *slog.Logger
	wake    chan struct{}
	running sync.WaitGroup

	mutex            sync.Mutex
	started          bool
	stopped          bool
	stop             context.CancelFunc
	leader           bool
	epoch            int64
	lockValue        string
	cancelLeadership context.CancelFunc
	leaseStartedAt   time.Time
}

// NewElection returns an Election that has not started yet, or an error if options are
// incomplete or inconsistent.
func NewElection(options *ElectionOptions) (*Election, error) {
	resolved, err := resolve(options)
	if err != nil {
		return nil, err
	}
	return &Election{
		options: resolved,
		logger:  cmp.Or(resolved.Logger, slog.New(slog.DiscardHandler)),
		wake:    make(chan struct{}, 1),
	}, nil
}

func resolve(options *ElectionOptions) (ElectionOptions, error) {
	if options == nil || options.Client == nil || options.LockKey == "" {
		return ElectionOptions{}, errors.New("leader: ElectionOptions.Client and LockKey are required")
	}
	resolved := *options
	resolved.InstanceID = cmp.Or(resolved.InstanceID, cryptorand.Text())
	resolved.RenewInterval = cmp.Or(resolved.RenewInterval, resolved.LeaseDuration/3)
	resolved.RetryInterval = cmp.Or(resolved.RetryInterval, resolved.LeaseDuration/2)
	if resolved.LeaseDuration < time.Millisecond {
		return ElectionOptions{}, fmt.Errorf("leader: LeaseDuration %s is shorter than a millisecond", resolved.LeaseDuration)
	}
	if resolved.RenewInterval <= 0 || 2*resolved.RenewInterval >= resolved.LeaseDuration {
		return ElectionOptions{}, fmt.Errorf("leader: RenewInterval %s must be positive and shorter than half of LeaseDuration %s", resolved.RenewInterval, resolved.LeaseDuration)
	}
	if resolved.RetryInterval <= 0 {
		return ElectionOptions{}, fmt.Errorf("leader: RetryInterval %s must be positive", resolved.RetryInterval)
	}
	return resolved, nil
}

// IsLeader reports whether this process holds the lock as far as it knows. A leader that cannot
// reach Redis steps down one RenewInterval before its lease may expire, so clocks that drift by
// less than that cannot make two processes lead at once; guard writes that must never overlap
// with FencingToken.
func (e *Election) IsLeader() bool {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	return e.leader
}

// FencingToken returns the epoch of the current leadership, which is larger than that of every
// earlier leadership, or zero when this process is not the leader. Storage that rejects writes
// carrying a smaller token than it has seen keeps a stale leader from overwriting a newer one.
func (e *Election) FencingToken() int64 {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	return e.epoch
}

// Start runs the election in the background until ctx ends or Stop is called. When ctx ends, a
// leader steps down and releases the lock; Stop is still needed to wait for the goroutines. It
// returns ErrAlreadyStarted on a second call.
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
					Client:    e.options.Client,
					Channel:   e.options.EventChannel,
					Logger:    e.logger,
					OnMessage: e.wakeFollower,
					// A follower that misses resignations still campaigns every RetryInterval.
					HeartbeatInterval: e.options.RetryInterval,
				})
			})
		})
	}
	e.running.Go(func() {
		recovery.Run(e.logger, "leader: election", func() { e.run(electionContext) })
	})
	return nil
}

// Stop ends the election and waits for its goroutines and OnElected to return. A leader then
// releases the lock and announces its resignation. Stop returns the error of releasing the lock;
// the lock then expires after LeaseDuration. Calling it again, or before Start, does nothing.
func (e *Election) Stop(ctx context.Context) error {
	e.mutex.Lock()
	if !e.started || e.stopped {
		e.mutex.Unlock()
		return nil
	}
	e.stopped = true
	e.mutex.Unlock()

	e.stop()
	e.running.Wait()

	lockValue, wasLeader := e.demote()
	if !wasLeader {
		return nil
	}
	return e.resign(ctx, lockValue)
}

func (e *Election) run(ctx context.Context) {
	for ctx.Err() == nil {
		if e.IsLeader() {
			e.holdLeadership(ctx)
		} else {
			e.campaign(ctx)
		}
	}
	e.resignUnlessStopping(ctx)
}

// resignUnlessStopping ends a leadership whose election ended because the context given to Start
// was cancelled. Stop resigns on its own, so it can return the error.
func (e *Election) resignUnlessStopping(ctx context.Context) {
	e.mutex.Lock()
	stopping := e.stopped
	e.mutex.Unlock()
	if stopping {
		return
	}
	heldLockValue, wasLeader := e.demote()
	if !wasLeader {
		return
	}
	resignContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.options.LeaseDuration)
	defer cancel()
	if err := e.resign(resignContext, heldLockValue); err != nil {
		e.logger.Warn("leader: resigning failed, the lock expires with its lease", "lock", e.options.LockKey, "error", err)
	}
}

// campaign tries to take the lock until it succeeds or ctx ends.
func (e *Election) campaign(ctx context.Context) {
	for !e.tryAcquire(ctx) {
		if !e.waitForRetry(ctx) {
			return
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
func (e *Election) holdLeadership(ctx context.Context) {
	ticker := time.NewTicker(e.options.RenewInterval)
	defer ticker.Stop()
	e.mutex.Lock()
	leaseStartedAt := e.leaseStartedAt
	e.mutex.Unlock()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		leadUntil := leaseStartedAt.Add(e.options.LeaseDuration - e.options.RenewInterval)
		attemptedAt := time.Now()
		held, err := e.renewBefore(ctx, leadUntil)
		switch {
		case ctx.Err() != nil:
			return
		case err == nil && held:
			leaseStartedAt = attemptedAt
		case err == nil:
			e.demote()
			return
		case !time.Now().Before(leadUntil):
			e.logger.Warn("leader: renewing the lease failed, resigning before it expires", "lock", e.options.LockKey, "error", err)
			e.demote()
			return
		default:
			e.logger.Warn("leader: renewing the lease failed", "lock", e.options.LockKey, "error", err)
		}
	}
}

// renewBefore gives up on a renewal at deadline, so a hanging Redis cannot keep this process
// believing it leads after its lease may have expired.
func (e *Election) renewBefore(ctx context.Context, deadline time.Time) (bool, error) {
	renewContext, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	return e.renew(renewContext)
}

func (e *Election) promote(ctx context.Context, epoch int64, leaseStartedAt time.Time) {
	leadershipContext, cancelLeadership := context.WithCancel(ctx)
	e.mutex.Lock()
	e.leader = true
	e.leaseStartedAt = leaseStartedAt
	e.epoch = epoch
	e.lockValue = lockValue(e.options.InstanceID, epoch)
	e.cancelLeadership = cancelLeadership
	e.mutex.Unlock()

	if e.options.OnElected != nil {
		e.running.Go(func() {
			recovery.Run(e.logger, "leader: OnElected", func() { e.options.OnElected(leadershipContext) })
		})
	}
}

// demote ends the leadership, if any, and returns the lock value it held.
func (e *Election) demote() (string, bool) {
	e.mutex.Lock()
	if !e.leader {
		e.mutex.Unlock()
		return "", false
	}
	heldLockValue := e.lockValue
	cancelLeadership := e.cancelLeadership
	e.leader = false
	e.epoch = 0
	e.lockValue = ""
	e.cancelLeadership = nil
	e.mutex.Unlock()

	cancelLeadership()
	if e.options.OnDemoted != nil {
		recovery.Run(e.logger, "leader: OnDemoted", e.options.OnDemoted)
	}
	return heldLockValue, true
}

func (e *Election) wakeFollower(string) {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func lockValue(instanceID string, epoch int64) string {
	return instanceID + ":" + strconv.FormatInt(epoch, 10)
}
