package redisadapter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEscapePattern(t *testing.T) {
	testCases := []struct {
		name     string
		text     string
		expected string
	}{
		{name: "plain", text: "users:alice", expected: "users:alice"},
		{name: "glob characters", text: `a*b?c[d]e\f`, expected: `a\*b\?c\[d\]e\\f`},
		{name: "valid multibyte UTF-8", text: "grüße*", expected: `grüße\*`},
		{name: "invalid UTF-8 survives byte for byte", text: "\xff\xfe", expected: "\xff\xfe"},
		{name: "invalid UTF-8 next to glob characters", text: "\xff*\xc3?", expected: "\xff\\*\xc3\\?"},
		{name: "truncated multibyte sequence", text: "\xe2\x82", expected: "\xe2\x82"},
		{name: "empty"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, escapePattern(testCase.text))
		})
	}
}
