package redisadapter_test

import (
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/govengers/cache"
	"github.com/govengers/cache/redisadapter"
	"github.com/stretchr/testify/require"
)

func TestLoadReadsOnlyItsNamespace(t *testing.T) {
	server := miniredis.RunT(t)
	client := newClient(t, server)
	first := newStore[string, string](t, &redisadapter.StoreOptions[string]{Client: client, KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "first"})
	second := newStore[string, string](t, &redisadapter.StoreOptions[string]{Client: client, KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "second"})
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
	store := newStore[string, string](t, &redisadapter.StoreOptions[string]{Client: newClient(t, server), KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "items"})

	entries, err := store.Load(t.Context())

	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestLoadSkipsUndecodableEntries(t *testing.T) {
	server := miniredis.RunT(t)
	logger, logged := newLogger()
	store := newStore[int, string](t, &redisadapter.StoreOptions[int]{
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
	store := newStore[string, string](t, &redisadapter.StoreOptions[string]{Client: client, KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "test"})
	other := newStore[string, string](t, &redisadapter.StoreOptions[string]{Client: client, KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "other"})
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

func TestLoadAndRemovePrefixWithoutKeyPrefix(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore[string, string](t, &redisadapter.StoreOptions[string]{Client: newClient(t, server), KeyCodec: cache.StringKeyCodec{}})
	ctx := t.Context()
	require.NoError(t, store.Set(ctx, "foo:fizz", "1"))
	require.NoError(t, store.Set(ctx, "bar:fizz", "2"))

	entries, err := store.Load(ctx)

	require.NoError(t, err)
	require.ElementsMatch(t, []cache.Entry[string, string]{{Key: "foo:fizz", Value: "1"}, {Key: "bar:fizz", Value: "2"}}, entries)
	require.NoError(t, store.RemovePrefix(ctx, "foo"))
	require.Equal(t, []string{"bar:fizz"}, server.Keys())
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
			store := newStore[string, string](t, &redisadapter.StoreOptions[string]{
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
	store := newStore[string, string](t, &redisadapter.StoreOptions[string]{Client: newClient(t, server), KeyCodec: cache.StringKeyCodec{}, KeyPrefix: "items"})
	ctx := t.Context()
	for _, key := range []string{"a*1", "ab1", "a?2", "a[b]3", "ab3"} {
		require.NoError(t, store.Set(ctx, key, "value"))
	}

	for _, prefix := range []string{"a*", "a?", "a[b]"} {
		require.NoError(t, store.RemovePrefix(ctx, prefix))
	}

	require.ElementsMatch(t, []string{"items:ab1", "items:ab3"}, server.Keys())
}
