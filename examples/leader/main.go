// Command leader runs two instances of a scheduler against one Redis: only the leader runs the
// jobs, and every write it makes carries the fencing token of its leadership. The job table
// refuses tokens older than the newest it has seen, so a leader that lost the lock without
// noticing (a network partition, a long GC pause) cannot overwrite its successor's work. In a
// real deployment every instance is its own process with one Election, and the table is a
// database row guarded by
//
//	UPDATE jobs SET state = $1, fencing_token = $2 WHERE id = $3 AND fencing_token <= $2
//
// REDIS_ADDR picks the server (default localhost:6379).
package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/govengers/cache/leader"
)

func main() {
	if err := runAgainstRedis(); err != nil {
		slog.Error("example failed", "error", err)
		os.Exit(1)
	}
}

func runAgainstRedis() error {
	client := redis.NewClient(&redis.Options{Addr: cmp.Or(os.Getenv("REDIS_ADDR"), "localhost:6379")})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return run(ctx, client, os.Stdout)
}

// leadership is one term of one instance, reported by OnElected.
type leadership struct {
	instance     string
	fencingToken int64
}

func run(ctx context.Context, client *redis.Client, out io.Writer) error {
	jobs := &jobTable{}
	elected := make(chan leadership, 2)

	first, err := startInstance(ctx, client, "first", jobs, elected)
	if err != nil {
		return err
	}
	defer first.Stop(context.WithoutCancel(ctx)) //nolint:errcheck // stopped explicitly below; a second Stop does nothing
	firstTerm, err := await(ctx, elected)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, firstTerm.instance, "leads")

	second, err := startInstance(ctx, client, "second", jobs, elected)
	if err != nil {
		return err
	}
	defer second.Stop(context.WithoutCancel(ctx)) //nolint:errcheck // stopped explicitly below; a second Stop does nothing

	// Stop releases the lock and announces it on the EventChannel, so the second instance takes
	// over at once instead of at its next RetryInterval.
	if err := first.Stop(ctx); err != nil {
		return err
	}
	secondTerm, err := await(ctx, elected)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, secondTerm.instance, "leads with a larger fencing token:", secondTerm.fencingToken > firstTerm.fencingToken)

	// A write of the first term arriving late, as from a leader that did not notice it was deposed.
	err = jobs.write(firstTerm.fencingToken)
	fmt.Fprintln(out, "late write of the first term:", err)
	return second.Stop(ctx)
}

func startInstance(ctx context.Context, client *redis.Client, instance string, jobs *jobTable, elected chan<- leadership) (*leader.Election, error) {
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
	return election, nil
}

func await(ctx context.Context, elected <-chan leadership) (leadership, error) {
	select {
	case term := <-elected:
		return term, nil
	case <-ctx.Done():
		return leadership{}, ctx.Err()
	}
}

var errStaleFencingToken = errors.New("refused: a newer leader has written")

// jobTable stands in for the database row the leader writes to.
type jobTable struct {
	mutex        sync.Mutex
	fencingToken int64
}

func (t *jobTable) write(fencingToken int64) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if fencingToken < t.fencingToken {
		return errStaleFencingToken
	}
	t.fencingToken = fencingToken
	return nil
}
