package leader

import (
	"context"
	"errors"
	"time"

	"github.com/govengers/cache/internal/opaque"
	"github.com/redis/go-redis/v9"
)

// The scripts run atomically in Redis, so no other client changes the lock between a check and
// the write that depends on it.
var (
	// acquireScript takes a free lock and returns the new epoch, or nil if the lock is held.
	// KEYS: lock, epoch counter. ARGV: instance ID, lease in milliseconds.
	acquireScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
	return nil
end
local epoch = redis.call('INCR', KEYS[2])
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

	// releaseScript deletes the lock if it still holds this lock value. KEYS: lock. ARGV: lock
	// value.
	releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
	redis.call('DEL', KEYS[1])
end
return 0
`)
)

// tryAcquire takes the lock if it is free and reports whether this process now leads.
func (e *Election) tryAcquire(ctx context.Context) bool {
	keys := []string{e.options.LockKey, e.options.LockKey + ":epoch"}
	attemptedAt := time.Now()
	epoch, err := acquireScript.Run(ctx, e.options.Client, keys, e.options.InstanceID, e.options.LeaseDuration.Milliseconds()).Int64()
	if errors.Is(err, redis.Nil) {
		return false
	}
	if err != nil {
		if ctx.Err() == nil {
			e.logger.Warn("leader: acquiring the lock failed", "lock", e.options.LockKey, "error", err)
		}
		return false
	}
	e.promote(ctx, epoch, attemptedAt)
	return true
}

// renew reports whether the lock still holds this process's lock value after extending it.
func (e *Election) renew(ctx context.Context) (bool, error) {
	e.mutex.Lock()
	heldLockValue := e.lockValue
	e.mutex.Unlock()

	renewed, err := renewScript.Run(ctx, e.options.Client, []string{e.options.LockKey}, heldLockValue, e.options.LeaseDuration.Milliseconds()).Int64()
	if err != nil {
		return false, err
	}
	return renewed == 1, nil
}

// resign releases the lock and announces it on the event channel.
func (e *Election) resign(ctx context.Context, heldLockValue string) error {
	if err := releaseScript.Run(ctx, e.options.Client, []string{e.options.LockKey}, heldLockValue).Err(); err != nil {
		return opaque.Error("leader: releasing lock "+e.options.LockKey, err)
	}
	if e.options.EventChannel == "" {
		return nil
	}
	if err := e.options.Client.Publish(ctx, e.options.EventChannel, e.options.InstanceID).Err(); err != nil {
		return opaque.Error("leader: announcing the resignation on "+e.options.EventChannel, err)
	}
	return nil
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
