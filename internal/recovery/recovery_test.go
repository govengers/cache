package recovery_test

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/govengers/cache/internal/recovery"
	"github.com/stretchr/testify/require"
)

func TestRun(t *testing.T) {
	testCases := []struct {
		name           string
		work           func()
		expectedLogged []string
	}{
		{
			name: "returning work logs nothing",
			work: func() {},
		},
		{
			name:           "panicking work is logged with operation, value and stack",
			work:           func() { panic("broken handler") },
			expectedLogged: []string{"level=ERROR", `msg="event handler panicked"`, "panic=\"broken handler\"", "stack=", "recovery_test.go"},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var logged bytes.Buffer
			ran := false

			recovery.Run(slog.New(slog.NewTextHandler(&logged, nil)), "event handler", func() {
				ran = true
				testCase.work()
			})

			require.True(t, ran)
			if testCase.expectedLogged == nil {
				require.Empty(t, logged.String())
			}
			for _, expected := range testCase.expectedLogged {
				require.Contains(t, logged.String(), expected)
			}
		})
	}
}
