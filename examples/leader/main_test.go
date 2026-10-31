package main

import (
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// TestRun hands the leadership from the first instance to the second and refuses the first term's
// late write.
func TestRun(t *testing.T) {
	defer goleak.VerifyNone(t)
	server, err := miniredis.Run()
	require.NoError(t, err)
	defer server.Close()
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	var out strings.Builder
	require.NoError(t, run(t.Context(), client, &out))
	require.Equal(t, "first leads\nsecond leads with a larger fencing token: true\nlate write of the first term: refused: a newer leader has written\n", out.String())
}
