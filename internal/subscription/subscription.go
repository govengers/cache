// Package subscription keeps a Redis Pub/Sub subscription alive across dropped connections and
// primary switches.
package subscription

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	minimumResubscribeDelay = 100 * time.Millisecond
	maximumResubscribeDelay = 10 * time.Second
)

// missedHeartbeats is how many heartbeat intervals may pass without the subscription's own
// heartbeat before it is renewed.
const missedHeartbeats = 3

// Parameters configures Keep.
type Parameters struct {
	Client  *redis.Client
	Channel string
	Logger  *slog.Logger
	// HeartbeatInterval is how often a numbered heartbeat is published, through Client's normal
	// connections, to a channel only this subscription listens on, together with a read of the
	// primary's replication ID. The subscription is renewed when the replication ID changes,
	// when a heartbeat is skipped, or when none arrived for three intervals: it may sit on a
	// former primary (go-redis moves normal connections after a Sentinel failover, not Pub/Sub
	// ones) that misses what the new primary publishes, or on a connection that died silently.
	// A handler that blocks OnMessage for three intervals also renews it.
	HeartbeatInterval time.Duration
	// OnSubscribed runs each time the subscription is established, before any message of it.
	// Optional.
	OnSubscribed func()
	// OnMessage runs with the payload of every message on Channel, one at a time.
	OnMessage func(payload string)
}

// Keep subscribes to the channel and hands every message to OnMessage until ctx ends. After a
// failure it resubscribes with exponential backoff and jitter; messages published in between
// are lost.
func Keep(ctx context.Context, parameters *Parameters) {
	delay := minimumResubscribeDelay
	for {
		if receiveUntilFailure(ctx, parameters) {
			delay = minimumResubscribeDelay
		}
		if !sleep(ctx, delay+rand.N(delay/5+1)) { //nolint:gosec // G404: jitter does not need a cryptographic RNG
			return
		}
		delay = min(2*delay, maximumResubscribeDelay)
	}
}

// receiveUntilFailure reports whether the subscription was established before it failed.
func receiveUntilFailure(ctx context.Context, parameters *Parameters) bool {
	heartbeatChannel := parameters.Channel + ":heartbeat:" + cryptorand.Text()
	pubSub := parameters.Client.Subscribe(ctx, parameters.Channel, heartbeatChannel)
	// Closing is what unblocks a pending receive when ctx ends; the second Close only reports
	// that it is already closed.
	stopClosing := context.AfterFunc(ctx, func() { _ = pubSub.Close() })
	defer stopClosing()
	defer func() { _ = pubSub.Close() }()

	// A connection that died silently would block an unbounded wait for the confirmation.
	if _, err := pubSub.ReceiveTimeout(ctx, missedHeartbeats*parameters.HeartbeatInterval); err != nil {
		logFailure(ctx, parameters, "redis: subscribing failed", err)
		return false
	}
	if parameters.OnSubscribed != nil {
		parameters.OnSubscribed()
	}
	heartbeat := &heartbeat{channel: heartbeatChannel, interval: parameters.HeartbeatInterval, lastReceivedAt: time.Now()}
	for {
		if heartbeat.missed(time.Now()) {
			parameters.Logger.Warn("redis: subscription missed its heartbeats, resubscribing", "channel", parameters.Channel)
			return true
		}
		if heartbeat.publishIfDue(ctx, parameters) {
			parameters.Logger.Warn("redis: the primary changed, resubscribing", "channel", parameters.Channel)
			return true
		}
		received, err := pubSub.ReceiveTimeout(ctx, parameters.HeartbeatInterval)
		if isTimeout(err) {
			continue
		}
		if err != nil {
			logFailure(ctx, parameters, "redis: subscription failed, resubscribing", err)
			return true
		}
		if heartbeat.dispatch(received, parameters.OnMessage) {
			parameters.Logger.Warn("redis: subscription lost a heartbeat, resubscribing", "channel", parameters.Channel)
			return true
		}
	}
}

func isTimeout(err error) bool {
	var networkError net.Error
	return errors.As(err, &networkError) && networkError.Timeout()
}

func logFailure(ctx context.Context, parameters *Parameters, message string, err error) {
	if ctx.Err() == nil {
		parameters.Logger.Warn(message, "channel", parameters.Channel, "error", err)
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
