package cache_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/govengers/cache"
	"github.com/govengers/cache/redisadapter"
	"github.com/redis/go-redis/v9"
)

func ExampleLocal() {
	sessions, err := cache.NewLocal[string, int](&cache.LocalOptions[string]{
		KeyCodec: cache.StringKeyCodec{},
		TTL:      time.Hour,
		Size:     10000,
	})
	if err != nil {
		panic(err)
	}

	sessions.Set("session:alice", 42)
	sessions.Set("session:bob", 7)
	sessions.RemovePrefix("session:b")

	userID, found := sessions.Get("session:alice")
	fmt.Println(userID, found)
	_, found = sessions.Get("session:bob")
	fmt.Println(found)
	// Output:
	// 42 true
	// false
}

func ExampleSynchronized() {
	server, err := miniredis.Run() // a real deployment points the client at Redis
	if err != nil {
		panic(err)
	}
	defer server.Close()
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	store, err := redisadapter.New[string, string](&redisadapter.Options[string]{
		Client:       client,
		KeyCodec:     cache.StringKeyCodec{},
		KeyPrefix:    "greetings",
		TTL:          time.Hour,
		EventChannel: "greetings:events",
	})
	if err != nil {
		panic(err)
	}
	defer store.Close()

	greetings, err := cache.NewSynchronized(&cache.SynchronizedOptions[string, string]{
		Backend:   store,
		LocalTTL:  time.Minute,
		LocalSize: 1000,
	})
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	defer func() {
		if err := greetings.Close(ctx); err != nil {
			panic(err)
		}
	}()
	if err := greetings.Set(ctx, "en", "hello"); err != nil {
		panic(err)
	}
	greeting, err := greetings.Get(ctx, "en")
	fmt.Println(greeting, err)
	_, err = greetings.Get(ctx, "fr")
	fmt.Println(errors.Is(err, cache.ErrNotFound))
	// Output:
	// hello <nil>
	// true
}
