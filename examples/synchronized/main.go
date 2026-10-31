// Command synchronized runs two replicas of a product catalog against one Redis. Each replica keeps
// a local cache.Synchronized in front of a shared redisadapter.Store, and a change one replica makes
// reaches the other's local cache through the store's event channel. In a real deployment every
// replica is its own process with one Store and one Synchronized cache per catalog. REDIS_ADDR
// picks the server (default localhost:6379).
package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/govengers/cache"
	"github.com/govengers/cache/redisadapter"
)

// Product is the cached value. Values are MessagePack-encoded in Redis, so exported fields only.
type Product struct {
	Name       string
	PriceCents int
}

func main() {
	if err := runAgainstRedis(); err != nil {
		slog.Error("example failed", "error", err)
		os.Exit(1)
	}
}

func runAgainstRedis() error {
	client := redis.NewClient(&redis.Options{Addr: cmp.Or(os.Getenv("REDIS_ADDR"), "localhost:6379")})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return run(ctx, client, os.Stdout)
}

func run(ctx context.Context, client *redis.Client, out io.Writer) (err error) {
	first, err := startReplica(ctx, client)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, first.close(context.WithoutCancel(ctx))) }()
	second, err := startReplica(ctx, client)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, second.close(context.WithoutCancel(ctx))) }()

	if err := first.products.Set(ctx, "desk", Product{Name: "desk", PriceCents: 24900}); err != nil {
		return err
	}
	desk, err := second.products.Get(ctx, "desk") // read from Redis, then kept in the second replica's local cache
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "second replica reads", desk.Name, desk.PriceCents)

	if err := first.products.Set(ctx, "desk", Product{Name: "desk", PriceCents: 19900}); err != nil {
		return err
	}
	// Events arrive asynchronously: the second replica sees the new price once its store received
	// the change, without reading Redis again.
	err = eventually(ctx, func() bool {
		desk, err := second.products.Get(ctx, "desk")
		return err == nil && desk.PriceCents == 19900
	})
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "second replica sees the new price 19900")

	if err := second.products.Remove(ctx, "desk"); err != nil {
		return err
	}
	err = eventually(ctx, func() bool {
		_, err := first.products.Get(ctx, "desk")
		return errors.Is(err, cache.ErrNotFound)
	})
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "first replica sees the removal")
	return nil
}

// replica is what one process of the application holds: one store and the cache in front of it.
type replica struct {
	store    *redisadapter.Store[string, Product]
	products *cache.Synchronized[string, Product]
}

func startReplica(ctx context.Context, client *redis.Client) (*replica, error) {
	store, err := redisadapter.New[string, Product](&redisadapter.Options[string]{
		Client:       client,
		KeyCodec:     cache.StringKeyCodec{},
		KeyPrefix:    "products",
		TTL:          time.Hour,
		EventChannel: "products:events",
	})
	if err != nil {
		return nil, err
	}
	products, err := cache.NewSynchronized(&cache.SynchronizedOptions[string, Product]{
		Backend:   store,
		LocalTTL:  time.Minute, // bounds how long a local copy outlives an entry Redis expired
		LocalSize: 1000,
	})
	if err != nil {
		return nil, errors.Join(err, store.Close(ctx))
	}
	started := &replica{store: store, products: products}
	// Preload only after the first subscription is up: its EventReset clears the local cache.
	if err := store.AwaitSubscription(ctx); err != nil {
		return nil, errors.Join(err, started.close(context.WithoutCancel(ctx)))
	}
	if err := products.Preload(ctx); err != nil {
		return nil, errors.Join(err, started.close(context.WithoutCancel(ctx)))
	}
	return started, nil
}

// close ends the cache before the store; the Redis client stays open for its owner to close last.
func (r *replica) close(ctx context.Context) error {
	return errors.Join(r.products.Close(ctx), r.store.Close(ctx))
}

func eventually(ctx context.Context, condition func() bool) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	return nil
}
