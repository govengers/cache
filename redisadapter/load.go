package redisadapter

import (
	"context"
	"strings"

	"github.com/govengers/cache"
	"github.com/govengers/cache/internal/opaque"
	"github.com/vmihailenco/msgpack/v5"
)

// Load returns every entry in the store's namespace. Entries whose key or value cannot be
// decoded are logged and skipped, so one stale entry does not block a preload. Under concurrent
// writes an entry may be missing or appear twice.
func (s *Store[K, V]) Load(ctx context.Context) ([]cache.Entry[K, V], error) {
	var entries []cache.Entry[K, V]
	err := s.scan(ctx, "", func(redisKeys []string) error {
		values, err := s.client.MGet(ctx, redisKeys...).Result()
		if err != nil {
			return err
		}
		entries = append(entries, s.decodeEntries(redisKeys, values)...)
		return nil
	})
	if err != nil {
		return nil, opaque.Error("redisadapter: load "+s.namespace, err)
	}
	return entries, nil
}

// scan calls handle with every page of Redis keys in the namespace whose encoding starts with
// prefix.
func (s *Store[K, V]) scan(ctx context.Context, prefix string, handle func(redisKeys []string) error) error {
	pattern := escapePattern(s.namespace+prefix) + "*"
	var cursor uint64
	for {
		redisKeys, next, err := s.client.Scan(ctx, cursor, pattern, s.scanCount).Result()
		if err != nil {
			return err
		}
		if len(redisKeys) > 0 {
			if err := handle(redisKeys); err != nil {
				return err
			}
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}

// escapePattern makes text match itself literally in a Redis glob pattern.
func escapePattern(text string) string {
	var escaped strings.Builder
	for _, character := range text {
		if strings.ContainsRune(`*?[]\`, character) {
			escaped.WriteRune('\\')
		}
		escaped.WriteRune(character)
	}
	return escaped.String()
}

// decodeEntries pairs the keys of a SCAN page with their MGET values. Keys that expired in
// between have a nil value and are skipped.
func (s *Store[K, V]) decodeEntries(redisKeys []string, values []any) []cache.Entry[K, V] {
	entries := make([]cache.Entry[K, V], 0, len(redisKeys))
	for index, redisKey := range redisKeys {
		encoded, found := values[index].(string)
		if !found {
			continue
		}
		entry, err := s.decodeEntry(redisKey, encoded)
		if err != nil {
			s.logger.Warn("redisadapter: skipped an undecodable entry", "key", redisKey, "error", err)
			continue
		}
		entries = append(entries, entry)
	}
	return entries
}

func (s *Store[K, V]) decodeEntry(redisKey, encoded string) (cache.Entry[K, V], error) {
	var entry cache.Entry[K, V]
	key, err := s.keyCodec.Decode(strings.TrimPrefix(redisKey, s.namespace))
	if err != nil {
		return entry, err
	}
	if err := msgpack.Unmarshal([]byte(encoded), &entry.Value); err != nil {
		return entry, err
	}
	entry.Key = key
	return entry, nil
}
