package redisadapter

import (
	"context"
	"slices"

	"github.com/govengers/cache"
	"github.com/govengers/cache/internal/opaque"
	"github.com/govengers/cache/internal/recovery"
	"github.com/govengers/cache/internal/subscription"
	"github.com/vmihailenco/msgpack/v5"
)

// eventMessage is the wire format of an event. The short field names keep messages small and
// must not change, or stores of different versions stop understanding each other.
type eventMessage[K comparable, V any] struct {
	Type   cache.EventType `msgpack:"t"`
	Key    K               `msgpack:"k,omitempty"`
	Value  V               `msgpack:"v,omitempty"`
	Prefix string          `msgpack:"p,omitempty"`
}

func (s *Store[K, V]) publish(ctx context.Context, message *eventMessage[K, V]) error {
	if s.eventChannel == "" {
		return nil
	}
	encoded, err := msgpack.Marshal(message)
	if err != nil {
		return opaque.Error("redisadapter: encode event", err)
	}
	if err := s.client.Publish(ctx, s.eventChannel, encoded).Err(); err != nil {
		return opaque.Error("redisadapter: publish event on "+s.eventChannel, err)
	}
	return nil
}

func (s *Store[K, V]) startReceiving() {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // G118: Close calls cancel through stopReceiving
	s.stopReceiving = cancel
	s.receiving.Go(func() {
		recovery.Run(s.logger, "redisadapter: receiving events", func() {
			subscription.Keep(ctx, &subscription.Parameters{
				Client:            s.client,
				Channel:           s.eventChannel,
				Logger:            s.logger,
				HeartbeatInterval: s.heartbeatInterval,
				OnSubscribed:      s.markSubscribed,
				OnMessage:         s.decodeAndDeliver,
			})
		})
	})
}

// markSubscribed delivers cache.EventReset each time the subscription is established, the first
// time included: events published before it was are lost, and copies made in the meantime may
// be stale.
func (s *Store[K, V]) markSubscribed() {
	s.deliver(cache.Event[K, V]{Type: cache.EventReset})
	s.firstSubscription.Do(func() { close(s.subscribed) })
}

func (s *Store[K, V]) decodeAndDeliver(payload string) {
	var message eventMessage[K, V]
	if err := msgpack.Unmarshal([]byte(payload), &message); err != nil {
		s.logger.Warn("redisadapter: dropped an undecodable event", "channel", s.eventChannel, "error", err)
		return
	}
	s.deliver(cache.Event[K, V]{Type: message.Type, Key: message.Key, Value: message.Value, Prefix: message.Prefix})
}

func (s *Store[K, V]) deliver(event cache.Event[K, V]) {
	s.handlersMutex.RLock()
	handlers := slices.Clone(s.handlers)
	s.handlersMutex.RUnlock()
	for _, handler := range handlers {
		recovery.Run(s.logger, "redisadapter: event handler", func() { handler(event) })
	}
}
