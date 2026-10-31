package redisadapter

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/govengers/cache"
	"github.com/govengers/cache/internal/recovery"
	"github.com/govengers/cache/internal/subscription"
	"github.com/vmihailenco/msgpack/v5"
)

// eventMessage is the wire format of an event. The short field names keep messages small and
// must not change, or stores of different versions stop understanding each other. Key is the
// key as the KeyCodec encodes it, Value the value as it is stored in Redis, so an event carries
// exactly what a read would return.
type eventMessage struct {
	Type   cache.EventType    `msgpack:"t"`
	Key    string             `msgpack:"k,omitempty"`
	Value  msgpack.RawMessage `msgpack:"v,omitempty"`
	Prefix string             `msgpack:"p,omitempty"`
}

func encodeEvent(message *eventMessage) ([]byte, error) {
	encoded, err := msgpack.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("encode event: %w", err)
	}
	return encoded, nil
}

type registeredHandler[K comparable, V any] struct {
	id     uint64
	handle func(cache.Event[K, V])
}

// OnEvent registers handler for every change published on the event channel, including this
// store's own, in the order Redis applied them. Handlers run one at a time on the receiving
// goroutine and must not block; a panicking handler is logged and skipped. A handler also
// receives cache.EventReset each time the subscription is established after it was
// registered, and whenever an event cannot be decoded. Without an event channel, handlers are
// never called. The returned function unregisters handler.
func (s *Store[K, V]) OnEvent(handler func(cache.Event[K, V])) (unregister func()) {
	s.handlersMutex.Lock()
	defer s.handlersMutex.Unlock()
	s.nextHandlerID++
	id := s.nextHandlerID
	s.handlers = append(s.handlers, registeredHandler[K, V]{id: id, handle: handler})
	return func() { s.unregister(id) }
}

func (s *Store[K, V]) unregister(id uint64) {
	s.handlersMutex.Lock()
	defer s.handlersMutex.Unlock()
	s.handlers = slices.DeleteFunc(s.handlers, func(registered registeredHandler[K, V]) bool {
		return registered.id == id
	})
}

func (s *Store[K, V]) startReceiving() {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // G118: Close calls cancel through stopReceiving
	s.stopReceiving = cancel
	parameters := &subscription.Parameters{
		Client:            s.client,
		Channel:           s.eventChannel,
		Logger:            s.logger,
		HeartbeatInterval: s.heartbeatInterval,
		OnSubscribed:      s.markSubscribed,
		OnMessage:         s.decodeAndDeliver,
	}
	s.receiving.Go(func() {
		// Keep returns only when ctx ends; after a panic it starts over, and the new
		// subscription resets the handlers' copies.
		for ctx.Err() == nil {
			recovery.Run(s.logger, "redisadapter: receiving events", func() { subscription.Keep(ctx, parameters) })
			sleep(ctx, restartDelay)
		}
	})
}

// markSubscribed delivers cache.EventReset each time the subscription is established, the first
// time included: events published before it was are lost, and copies made in the meantime may
// be stale.
func (s *Store[K, V]) markSubscribed() {
	s.deliver(cache.Event[K, V]{Type: cache.EventReset})
	s.firstSubscription.Do(func() { close(s.subscribed) })
}

// decodeAndDeliver turns an event it cannot decode into cache.EventReset: dropping it would
// leave the change it stands for unapplied.
func (s *Store[K, V]) decodeAndDeliver(payload string) {
	event, err := s.decodeEvent(payload)
	if err != nil {
		s.logger.Warn("redisadapter: undecodable event, resetting", "channel", s.eventChannel, "error", err)
		event = cache.Event[K, V]{Type: cache.EventReset}
	}
	s.deliver(event)
}

// restartDelay keeps a reception that panics at once from spinning.
const restartDelay = time.Second

// sleep waits for duration or until ctx ends.
func sleep(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

func (s *Store[K, V]) decodeEvent(payload string) (cache.Event[K, V], error) {
	var message eventMessage
	if err := msgpack.Unmarshal([]byte(payload), &message); err != nil {
		return cache.Event[K, V]{}, err
	}
	event := cache.Event[K, V]{Type: message.Type, Prefix: message.Prefix}
	switch message.Type {
	case cache.EventSet, cache.EventRemove:
		key, err := s.keyCodec.Decode(message.Key)
		if err != nil {
			return event, err
		}
		event.Key = key
	case cache.EventRemovePrefix, cache.EventReset:
	default:
		return event, fmt.Errorf("unknown event type %d", message.Type)
	}
	if message.Type == cache.EventSet {
		if err := msgpack.Unmarshal(message.Value, &event.Value); err != nil {
			return event, err
		}
	}
	return event, nil
}

func (s *Store[K, V]) deliver(event cache.Event[K, V]) {
	s.handlersMutex.Lock()
	handlers := make([]func(cache.Event[K, V]), len(s.handlers))
	for index, registered := range s.handlers {
		handlers[index] = registered.handle
	}
	s.handlersMutex.Unlock()
	for _, handler := range handlers {
		recovery.Run(s.logger, "redisadapter: event handler", func() { handler(event) })
	}
}
