package redisadapter_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/govengers/cache"
	"github.com/govengers/cache/redisadapter"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestLoadReadsOnlyItsNamespace(t *testing.T) {
	server := miniredis.RunT(t)
	client := newClient(t, server)
	first := newStore[string, string](t, &redisadapter.Options[string]{Client: client, KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "first"})
	second := newStore[string, string](t, &redisadapter.Options[string]{Client: client, KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "second"})
	ctx := t.Context()
	require.NoError(t, first.Set(ctx, "foo:fizz", "1"))
	require.NoError(t, first.Set(ctx, "bar", "2"))
	require.NoError(t, second.Set(ctx, "foo:fizz", "3"))
	require.NoError(t, server.Set("firstly", encode(t, "outside")))

	entries, err := first.Load(ctx)

	require.NoError(t, err)
	require.ElementsMatch(t, []cache.Entry[string, string]{{Key: "foo:fizz", Value: "1"}, {Key: "bar", Value: "2"}}, entries)
}

func TestLoadOfEmptyNamespaceIsEmpty(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore[string, string](t, &redisadapter.Options[string]{Client: newClient(t, server), KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "items"})

	entries, err := store.Load(t.Context())

	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestLoadSkipsUndecodableEntries(t *testing.T) {
	server := miniredis.RunT(t)
	logger, logged := newLogger()
	store := newStore[int, string](t, &redisadapter.Options[int]{
		Client:    newClient(t, server),
		KeyCodec:  cache.IntKeyCodec{},
		KeyPrefix: "numbers",
		Logger:    logger,
	})
	require.NoError(t, store.Set(t.Context(), 1, "one"))
	require.NoError(t, server.Set("numbers:two", encode(t, "undecodable key")))
	require.NoError(t, server.Set("numbers:3", "\xc1"))
	server.HSet("numbers:4", "field", "not a string, so MGET returns nil")

	entries, err := store.Load(t.Context())

	require.NoError(t, err)
	require.Equal(t, []cache.Entry[int, string]{{Key: 1, Value: "one"}}, entries)
	require.Equal(t, 2, strings.Count(logged.String(), "redisadapter: skipped an undecodable entry"))
	require.Contains(t, logged.String(), "key=numbers:two")
	require.Contains(t, logged.String(), "key=numbers:3")
}

func TestRemovePrefixRemovesOnlyMatchingKeysInItsNamespace(t *testing.T) {
	server := miniredis.RunT(t)
	client := newClient(t, server)
	store := newStore[string, string](t, &redisadapter.Options[string]{Client: client, KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "test"})
	other := newStore[string, string](t, &redisadapter.Options[string]{Client: client, KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "other"})
	ctx := t.Context()
	for _, key := range []string{"foo:fizz", "foo:buzz", "bar:fizz", "bar:buzz"} {
		require.NoError(t, store.Set(ctx, key, "value"))
	}
	require.NoError(t, other.Set(ctx, "foo:fizz", "value"))

	require.NoError(t, store.RemovePrefix(ctx, "foo"))

	require.ElementsMatch(t, []string{"test:bar:fizz", "test:bar:buzz", "other:foo:fizz"}, server.Keys())

	require.NoError(t, store.RemovePrefix(ctx, ""))

	require.Equal(t, []string{"other:foo:fizz"}, server.Keys())
}

func TestGlobCharactersInKeyPrefixMatchLiterally(t *testing.T) {
	testCases := []struct {
		name      string
		keyPrefix string
		// neighbor is a key outside the namespace that the prefix would match as a glob.
		neighbor string
	}{
		{name: "star", keyPrefix: "tenant*", neighbor: "tenantX:key"},
		{name: "question mark", keyPrefix: "tenant?", neighbor: "tenantX:key"},
		{name: "brackets", keyPrefix: "tenant[ab]", neighbor: "tenanta:key"},
		{name: "backslash", keyPrefix: `tenant\`, neighbor: "tenant:key"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := miniredis.RunT(t)
			store := newStore[string, string](t, &redisadapter.Options[string]{
				Client:    newClient(t, server),
				KeyCodec:  cache.StringKeyCodec{},
				KeyPrefix: testCase.keyPrefix,
			})
			ctx := t.Context()
			require.NoError(t, store.Set(ctx, "key", "inside"))
			require.NoError(t, server.Set(testCase.neighbor, encode(t, "outside")))

			entries, err := store.Load(ctx)

			require.NoError(t, err)
			require.Equal(t, []cache.Entry[string, string]{{Key: "key", Value: "inside"}}, entries)
			require.NoError(t, store.RemovePrefix(ctx, ""))
			require.Equal(t, []string{testCase.neighbor}, server.Keys())
		})
	}
}

func TestGlobCharactersInRemovedPrefixMatchLiterally(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore[string, string](t, &redisadapter.Options[string]{Client: newClient(t, server), KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "items"})
	ctx := t.Context()
	for _, key := range []string{"a*1", "ab1", "a?2", "a[b]3", "ab3"} {
		require.NoError(t, store.Set(ctx, key, "value"))
	}

	for _, prefix := range []string{"a*", "a?", "a[b]"} {
		require.NoError(t, store.RemovePrefix(ctx, prefix))
	}

	require.ElementsMatch(t, []string{"items:ab1", "items:ab3"}, server.Keys())
}

// pagingHook makes SCAN return one key per page, as Redis may: miniredis ignores COUNT and
// returns every key at once. A scan starting at cursor 0 takes a snapshot of the matching keys,
// so keys removed in between do not shift the later pages; every later cursor continues it. It
// is for one goroutine at a time.
type pagingHook struct {
	snapshot []string
	position int
	pages    int
}

const continueCursor = 1

func (*pagingHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *pagingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		scan, isScan := command.(*redis.ScanCmd)
		if !isScan {
			return next(ctx, command)
		}
		if fmt.Sprint(scan.Args()[1]) == "0" {
			full := redis.NewScanCmd(ctx, nil, scan.Args()...)
			if err := next(ctx, full); err != nil {
				return err
			}
			h.snapshot, _ = full.Val()
			h.position = 0
		}
		h.pages++
		page := h.snapshot[h.position:min(h.position+1, len(h.snapshot))]
		h.position += len(page)
		if h.position < len(h.snapshot) {
			scan.SetVal(page, continueCursor)
		} else {
			scan.SetVal(page, 0)
		}
		return nil
	}
}

func (*pagingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestLoadAndRemovePrefixFollowScanCursors(t *testing.T) {
	server := miniredis.RunT(t)
	paging := &pagingHook{}
	store := newStore[string, string](t, &redisadapter.Options[string]{
		Client:    newClient(t, server, paging),
		KeyCodec:  cache.StringKeyCodec{},
		KeyPrefix: "items",
		ScanCount: 1,
	})
	ctx := t.Context()
	for _, key := range []string{"a", "b", "c"} {
		require.NoError(t, store.Set(ctx, key, "value "+key))
	}

	entries, err := store.Load(ctx)

	require.NoError(t, err)
	require.ElementsMatch(t, []cache.Entry[string, string]{{Key: "a", Value: "value a"}, {Key: "b", Value: "value b"}, {Key: "c", Value: "value c"}}, entries)
	require.Equal(t, 3, paging.pages)
	require.NoError(t, store.RemovePrefix(ctx, ""))
	require.Empty(t, server.Keys())
}
