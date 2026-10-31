package cache_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
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

type tenantKey struct {
	Tenant string
	ID     int
}

// tenantKeyCodec encodes a tenantKey as "<tenant>:<id>"; tenants must not contain a colon.
type tenantKeyCodec struct{}

func (tenantKeyCodec) Encode(key tenantKey) string {
	return key.Tenant + ":" + strconv.Itoa(key.ID)
}

func (tenantKeyCodec) Decode(encoded string) (tenantKey, error) {
	tenant, id, found := strings.Cut(encoded, ":")
	if !found {
		return tenantKey{}, fmt.Errorf("tenant key %q has no colon", encoded)
	}
	number, err := strconv.Atoi(id)
	return tenantKey{Tenant: tenant, ID: number}, err
}

func ExampleKeyCodec() {
	orders, err := cache.NewLocal[tenantKey, string](&cache.LocalOptions[tenantKey]{KeyCodec: tenantKeyCodec{}})
	if err != nil {
		panic(err)
	}
	orders.Set(tenantKey{Tenant: "acme", ID: 1}, "desk")
	orders.Set(tenantKey{Tenant: "globex", ID: 1}, "chair")
	orders.RemovePrefix("acme:") // every order of one tenant

	fmt.Println(orders.Contains(tenantKey{Tenant: "acme", ID: 1}), orders.Contains(tenantKey{Tenant: "globex", ID: 1}))
	// Output:
	// false true
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
	defer func() {
		if err := store.Close(context.Background()); err != nil {
			panic(err)
		}
	}()

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
