package main

import (
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// TestRun propagates a change and a removal between the two replicas through miniredis.
func TestRun(t *testing.T) {
	defer goleak.VerifyNone(t)
	server, err := miniredis.Run()
	require.NoError(t, err)
	defer server.Close()
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	var out strings.Builder
	require.NoError(t, run(t.Context(), client, &out))
	require.Equal(t, "second replica reads desk 24900\nsecond replica sees the new price 19900\nfirst replica sees the removal\n", out.String())
}
