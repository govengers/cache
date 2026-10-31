package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRun shares a write and a prefix removal between two caches on the memory backend.
func TestRun(t *testing.T) {
	var out strings.Builder
	require.NoError(t, run(t.Context(), &out))
	require.Equal(t, "second reads 1 <nil>\nafter the prefix removal: cache: not found\n", out.String())
}
