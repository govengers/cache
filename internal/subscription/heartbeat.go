package subscription

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// heartbeat numbers what it publishes: a gap in what arrives means the subscription missed
// messages, as it does after a failover until the former primary rejoins as a replica. With
// each heartbeat it also reads the primary's replication ID, which changes when another server
// is promoted or the primary restarts without its data, so a failover is noticed even when no
// heartbeat fell into the gap.
type heartbeat struct {
	channel         string
	interval        time.Duration
	lastPublishedAt time.Time
	lastReceivedAt  time.Time
	published       uint64
	received        uint64
	primaryID       string
}

func (h *heartbeat) missed(now time.Time) bool {
	return now.Sub(h.lastReceivedAt) > missedHeartbeats*h.interval
}

// publishIfDue publishes the next heartbeat once an interval has passed and reports whether
// the primary is no longer the server the subscription started on.
func (h *heartbeat) publishIfDue(ctx context.Context, parameters *Parameters) bool {
	if time.Since(h.lastPublishedAt) < h.interval {
		return false
	}
	h.lastPublishedAt = time.Now()
	publishContext, cancel := context.WithTimeout(ctx, h.interval)
	defer cancel()
	pipeline := parameters.Client.Pipeline()
	publish := pipeline.Publish(publishContext, h.channel, strconv.FormatUint(h.published+1, 10))
	replication := pipeline.Info(publishContext, "replication")
	// Exec repeats the first command error; each command is checked on its own below.
	_, _ = pipeline.Exec(publishContext)
	if err := publish.Err(); err != nil {
		// The number is reused, so a heartbeat that was delivered despite the error is not
		// taken for a gap.
		logFailure(ctx, parameters, "redis: publishing the subscription heartbeat failed", err)
	} else {
		h.published++
	}
	// A server that does not report a replication ID (an error, or a fake in tests) leaves the
	// gap and timeout checks, which still catch a lost subscription.
	return replication.Err() == nil && h.primaryChanged(replicationID(replication.Val()))
}

func (h *heartbeat) primaryChanged(primaryID string) bool {
	if primaryID == "" {
		return false
	}
	if h.primaryID == "" {
		h.primaryID = primaryID
		return false
	}
	return primaryID != h.primaryID
}

// replicationID returns master_replid from the reply of INFO replication, or "".
func replicationID(info string) string {
	for line := range strings.Lines(info) {
		if value, found := strings.CutPrefix(strings.TrimSpace(line), "master_replid:"); found {
			return value
		}
	}
	return ""
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
