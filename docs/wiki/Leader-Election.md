# Leader election

`leader.Election`: one leader among processes sharing a Redis, with a lease the leader renews and a fencing token that grows with every new leadership.

## Usage

From [examples/leader/main.go](../../examples/leader/main.go) (tested by its `main_test.go`, which hands the leadership from one instance to another and refuses the old term's late write):

```go
	election, err := leader.New(&leader.Options{
		Client:        client,
		LockKey:       "examples:scheduler",
		InstanceID:    instance,
		LeaseDuration: 2 * time.Second, // a crashed leader's lock expires after at most this long
		EventChannel:  "examples:scheduler:resignations",
		OnElected: func(ctx context.Context, fencingToken int64) {
			// Real work runs here until ctx ends; every write carries fencingToken.
			if err := jobs.write(fencingToken); err != nil {
				return
			}
			elected <- leadership{instance: instance, fencingToken: fencingToken}
			<-ctx.Done() // must return when the leadership ends
		},
	})
	if err != nil {
		return nil, err
	}
	if err := election.Start(ctx); err != nil {
		return nil, err
	}
```

Then `defer election.Stop(context.WithoutCancel(ctx))`: it releases the lock and waits for `OnElected` to return.

## Options

- `Client` (required): `*redis.Client`; you close it after `Stop`.
- `LockKey` (required): the lock; the fencing token counter lives at `<LockKey>:epoch`.
- `LeaseDuration` (required, whole milliseconds): a crashed leader's lock expires after at most this long.
- `RenewInterval`: default a third of the lease; must be below half of it.
- `RetryInterval`: follower attempts, plus up to a quarter as jitter. Default half the lease, at most a day.
- `InstanceID`: written into the lock value. Default random.
- `OnElected(ctx, fencingToken)`: own goroutine per term; `ctx` ends with the leadership. Must return then and must not call `Stop`.
- `OnDemoted()`: after a term ends; must not block or call `Stop`.
- `EventChannel`: a resigning leader announces itself so followers take over at once instead of at their next retry. Followers keep their subscription alive with a heartbeat every `RetryInterval`.
- `Logger`: recovered Redis failures and panics in the callbacks.

## Lifecycle

- `Start(ctx)` runs the election in the background; a second call returns `ErrAlreadyStarted`. When `ctx` ends, a leader steps down and releases the lock; call `Stop` anyway to wait for the goroutines.
- `Stop(ctx)` ends the election, releases the lock, announces the resignation and waits for `OnElected`. It returns the release error; the lock then expires after `LeaseDuration`. Calling it again or before `Start` does nothing.
- `IsLeader()` and `FencingToken()` (zero when not leading) report the current state.
- A new term's `OnElected` never starts before the previous one returned.
- An election does not campaign again after a panic in the election itself; callbacks recover on their own.

## Timing

The leader renews every `RenewInterval`. It steps down when the lock was taken over, or when no renewal succeeded by one `RenewInterval` before the lease may expire, even while Redis hangs (it stops waiting for Redis at that deadline, whatever the client's timeouts). Clocks drifting by less than one `RenewInterval` therefore cannot make two processes lead at once.

## Fencing tokens

A leader cut off from Redis can believe it leads for up to one lease minus one `RenewInterval`, a paused process (GC, frozen VM) for longer. Every write that must never overlap carries the token `OnElected` received, and storage refuses tokens smaller than the largest it has seen:

```sql
UPDATE jobs SET state = $1, fencing_token = $2 WHERE id = $3 AND fencing_token <= $2
```

Work of a term keeps using the token it was given; `FencingToken()` returns the current term's and can change under long-running work.

## Sentinel caveat

Replication is asynchronous: a failover right after an acquisition can lose the lock and the epoch counter, so a second leader can get a fencing token that was already issued. Tokens are monotonic only while no write is lost to a failover. Redis Cluster is not supported (the scripts touch two keys).

Source: [leader/election.go](../../leader/election.go), [leader/lock.go](../../leader/lock.go).
