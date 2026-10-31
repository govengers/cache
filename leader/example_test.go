package leader_test

import (
	"context"
	"fmt"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/govengers/cache/leader"
	"github.com/redis/go-redis/v9"
)

func ExampleElection() {
	server, err := miniredis.Run() // a real deployment points the client at Redis
	if err != nil {
		panic(err)
	}
	defer server.Close()
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	elected := make(chan struct{})
	election, err := leader.NewElection(&leader.ElectionOptions{
		Client:        client,
		LockKey:       "billing:scheduler",
		LeaseDuration: 10 * time.Second,
		EventChannel:  "billing:scheduler:resignations",
		OnElected: func(ctx context.Context) {
			close(elected)
			<-ctx.Done() // lead until the leadership ends
		},
	})
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	if err := election.Start(ctx); err != nil {
		panic(err)
	}
	<-elected
	fmt.Println(election.IsLeader(), election.FencingToken())

	if err := election.Stop(ctx); err != nil {
		panic(err)
	}
	fmt.Println(election.IsLeader())
	// Output:
	// true 1
	// false
}
