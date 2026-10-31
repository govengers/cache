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
	"strconv"
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
	// connections, to a channel only this subscription listens on. A subscription that skips a
	// heartbeat, or has received none for three intervals, is renewed: it may sit on a former
	// primary (go-redis moves normal connections after a Sentinel failover, not Pub/Sub ones),
	// which misses what the new primary publishes until it rejoins as a replica, or on a
	// connection that died silently. A handler that blocks OnMessage for three intervals also
	// renews it.
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

	if _, err := pubSub.Receive(ctx); err != nil {
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
		heartbeat.publishIfDue(ctx, parameters)
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

// heartbeat numbers what it publishes: a gap in what arrives means the subscription missed
// messages, as it does after a failover until the former primary rejoins as a replica, even
// though later heartbeats arrive in time.
type heartbeat struct {
	channel         string
	interval        time.Duration
	lastPublishedAt time.Time
	lastReceivedAt  time.Time
	published       uint64
	received        uint64
}

func (h *heartbeat) missed(now time.Time) bool {
	return now.Sub(h.lastReceivedAt) > missedHeartbeats*h.interval
}

func (h *heartbeat) publishIfDue(ctx context.Context, parameters *Parameters) {
	if time.Since(h.lastPublishedAt) < h.interval {
		return
	}
	h.lastPublishedAt = time.Now()
	publishContext, cancel := context.WithTimeout(ctx, h.interval)
	defer cancel()
	err := parameters.Client.Publish(publishContext, h.channel, strconv.FormatUint(h.published+1, 10)).Err()
	if err != nil {
		// The number is reused, so a heartbeat that was delivered despite the error is not
		// taken for a gap.
		logFailure(ctx, parameters, "redis: publishing the subscription heartbeat failed", err)
		return
	}
	h.published++
}

// dispatch records a heartbeat and hands a message on the subscribed channel to onMessage;
// subscription confirmations and pongs carry nothing. It reports whether a heartbeat was lost.
func (h *heartbeat) dispatch(received any, onMessage func(payload string)) bool {
	message, isMessage := received.(*redis.Message)
	switch {
	case !isMessage:
		return false
	case message.Channel == h.channel:
		return h.record(message.Payload)
	default:
		onMessage(message.Payload)
		return false
	}
}

func (h *heartbeat) record(payload string) bool {
	number, err := strconv.ParseUint(payload, 10, 64)
	if err != nil || number <= h.received {
		return false
	}
	lost := number > h.received+1
	h.received = number
	h.lastReceivedAt = time.Now()
	return lost
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
