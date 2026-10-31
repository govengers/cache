package leader

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/govengers/cache/internal/opaque"
	"github.com/govengers/cache/internal/recovery"
	"github.com/redis/go-redis/v9"
)

// The scripts run atomically in Redis, so no other client changes the lock between a check and
// the write that depends on it.
var (
	// acquireScript takes a free lock and returns the new epoch as a decimal string, or nil if
	// the lock is held. The epoch is read back with GET instead of using INCR's reply, which Lua
	// would turn into a float. KEYS: lock, epoch counter. ARGV: instance ID, lease in
	// milliseconds.
	acquireScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
	return nil
end
redis.call('INCR', KEYS[2])
local epoch = redis.call('GET', KEYS[2])
redis.call('SET', KEYS[1], ARGV[1] .. ':' .. epoch, 'PX', ARGV[2])
return epoch
`)

	// renewScript extends the lease and returns 1 if the lock still holds this lock value,
	// otherwise 0. KEYS: lock. ARGV: lock value, lease in milliseconds.
	renewScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
	redis.call('PEXPIRE', KEYS[1], ARGV[2])
	return 1
end
return 0
`)

	// releaseScript deletes the lock and returns 1 if it still holds this lock value, otherwise
	// 0. KEYS: lock. ARGV: lock value.
	releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
	redis.call('DEL', KEYS[1])
	return 1
end
return 0
`)
)

var (
	errDeadlinePassed = errors.New("leader: no reply before the lease deadline")
	errCallInFlight   = errors.New("leader: an abandoned Redis call is still in flight")
)

// tryAcquire takes the lock if it is free and returns the new leadership, or nil. A reply that
// arrives so late that the lease may expire before the first renewal does not count.
func (e *Election) tryAcquire(ctx context.Context) *leadership {
	keys := []string{e.options.LockKey, e.options.LockKey + ":epoch"}
	attemptedAt := time.Now()
	acquire := func(ctx context.Context) (string, error) {
		return acquireScript.Run(ctx, e.options.Client, keys, e.options.InstanceID, e.options.LeaseDuration.Milliseconds()).Text()
	}
	// A lock taken by a reply that came too late must not block every process until it expires.
	releaseLateLock := func(epoch string) { e.resignInBackground(e.options.InstanceID + ":" + epoch) }
	epoch, err := callBefore(ctx, e, e.leadUntil(attemptedAt), acquire, releaseLateLock)
	if errors.Is(err, redis.Nil) || ctx.Err() != nil {
		return nil
	}
	if err != nil {
		e.logger.Warn("leader: acquiring the lock failed", "lock", e.options.LockKey, "error", err)
		return nil
	}
	fencingToken, err := strconv.ParseInt(epoch, 10, 64)
	if err != nil {
		e.logger.Warn("leader: the epoch counter holds no integer", "lock", e.options.LockKey, "epoch", epoch)
		return nil
	}
	return e.promote(ctx, fencingToken, e.options.InstanceID+":"+epoch, attemptedAt)
}

// renew reports whether the lock still holds lockValue after extending it, and gives up at
// deadline.
func (e *Election) renew(ctx context.Context, lockValue string, deadline time.Time) (bool, error) {
	renewed, err := callBefore(ctx, e, deadline, func(ctx context.Context) (int64, error) {
		return renewScript.Run(ctx, e.options.Client, []string{e.options.LockKey}, lockValue, e.options.LeaseDuration.Milliseconds()).Int64()
	}, nil)
	if err != nil {
		return false, err
	}
	return renewed == 1, nil
}

// resign releases the lock and announces it on the event channel.
func (e *Election) resign(ctx context.Context, lockValue string) error {
	released, err := releaseScript.Run(ctx, e.options.Client, []string{e.options.LockKey}, lockValue).Int64()
	if err != nil {
		return opaque.Error("leader: releasing lock "+e.options.LockKey, err)
	}
	if released == 0 {
		e.logger.Warn("leader: the lock was taken over before it was released", "lock", e.options.LockKey)
		return nil
	}
	if e.options.EventChannel == "" {
		return nil
	}
	if err := e.options.Client.Publish(ctx, e.options.EventChannel, e.options.InstanceID).Err(); err != nil {
		return opaque.Error("leader: announcing the resignation on "+e.options.EventChannel, err)
	}
	return nil
}

type callResult[T any] struct {
	value T
	err   error
}

// callBefore runs call and stops waiting for it at deadline. go-redis bounds socket I/O by its
// own timeouts, not by the context, unless the client sets ContextTimeoutEnabled; a lease must
// not depend on that. A result that arrives too late counts as no result: discard, if any, gets
// a late successful value to undo its effect. An abandoned call keeps running until those
// timeouts end, holding the call slot; Stop waits for it.
func callBefore[T any](ctx context.Context, election *Election, deadline time.Time, call func(ctx context.Context) (T, error), discard func(T)) (T, error) {
	var zero T
	select {
	case election.callSlot <- struct{}{}:
	default:
		return zero, errCallInFlight
	}
	callContext, cancel := context.WithDeadline(ctx, deadline)
	results := make(chan callResult[T], 1)
	election.running.Go(func() {
		result := callResult[T]{err: errors.New("leader: redis call panicked")}
		recovery.Run(election.logger, "leader: redis call", func() {
			value, err := call(callContext)
			result = callResult[T]{value: value, err: err}
		})
		cancel()
		<-election.callSlot // free before the result is seen, so the next call can follow at once
		results <- result
	})
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case result := <-results:
		if time.Now().Before(deadline) && ctx.Err() == nil {
			return result.value, result.err
		}
		discardSuccess(result, discard)
		return zero, errDeadlinePassed
	case <-timer.C:
		discardWhenDone(election, results, discard)
		return zero, errDeadlinePassed
	case <-ctx.Done():
		discardWhenDone(election, results, discard)
		return zero, fmt.Errorf("leader: redis call: %w", ctx.Err())
	}
}

// discardWhenDone waits in the background for an abandoned call and discards its result.
func discardWhenDone[T any](election *Election, results <-chan callResult[T], discard func(T)) {
	if discard == nil {
		return
	}
	election.running.Go(func() {
		recovery.Run(election.logger, "leader: discarding a late reply", func() { discardSuccess(<-results, discard) })
	})
}

func discardSuccess[T any](result callResult[T], discard func(T)) {
	if result.err == nil && discard != nil {
		discard(result.value)
	}
}

// sleep waits for duration and reports false if ctx ended first.
func sleep(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
