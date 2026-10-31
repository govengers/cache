package opaque_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/govengers/cache/internal/opaque"
	"github.com/stretchr/testify/require"
)

var errLibrary = errors.New("library failure")

func TestError(t *testing.T) {
	testCases := []struct {
		name            string
		err             error
		expectedMessage string
		matches         error
	}{
		{
			name:            "canceled context stays matchable",
			err:             fmt.Errorf("dial: %w", context.Canceled),
			expectedMessage: "redis: get: context canceled",
			matches:         context.Canceled,
		},
		{
			name:            "exceeded deadline stays matchable",
			err:             fmt.Errorf("read: %w", context.DeadlineExceeded),
			expectedMessage: "redis: get: context deadline exceeded",
			matches:         context.DeadlineExceeded,
		},
		{
			name:            "library error keeps its message",
			err:             fmt.Errorf("read: %w", errLibrary),
			expectedMessage: "redis: get: read: library failure",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := opaque.Error("redis: get", testCase.err)

			require.EqualError(t, err, testCase.expectedMessage)
			require.NotErrorIs(t, err, errLibrary)
			if testCase.matches != nil {
				require.ErrorIs(t, err, testCase.matches)
			} else {
				require.NoError(t, errors.Unwrap(err))
			}
		})
	}
}
