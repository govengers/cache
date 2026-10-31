package redisadapter_test

import (
	"context"
	"fmt"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/govengers/cache"
	"github.com/govengers/cache/redisadapter"
	"github.com/redis/go-redis/v9"
)

func ExampleNew() {
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
	ctx := context.Background()
	defer func() {
		if err := store.Close(ctx); err != nil {
			panic(err)
		}
	}()

	changes := make(chan cache.Event[string, string], 1)
	unregister := store.OnEvent(func(event cache.Event[string, string]) {
		if event.Type == cache.EventSet {
			changes <- event
		}
	})
	defer unregister()
	if err := store.AwaitSubscription(ctx); err != nil {
		panic(err)
	}

	if err := store.Set(ctx, "en", "hello"); err != nil {
		panic(err)
	}
	greeting, err := store.Get(ctx, "en")
	fmt.Println(greeting, err)
	change := <-changes
	fmt.Println(change.Key, change.Value)
	// Output:
	// hello <nil>
	// en hello
}
