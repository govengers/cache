package cache_test

import (
	"testing"

	"github.com/govengers/cache"
	"github.com/stretchr/testify/require"
)

func TestStringKeyCodecRoundTrip(t *testing.T) {
	for _, key := range []string{"", "key", "user:1", "with spaces*?"} {
		t.Run(key, func(t *testing.T) {
			codec := cache.StringKeyCodec{}
			require.Equal(t, key, codec.Encode(key))
			decoded, err := codec.Decode(codec.Encode(key))
			require.NoError(t, err)
			require.Equal(t, key, decoded)
		})
	}
}

func TestIntKeyCodecRoundTrip(t *testing.T) {
	testCases := []struct {
		name    string
		key     int
		encoded string
	}{
		{name: "zero", key: 0, encoded: "0"},
		{name: "positive", key: 42, encoded: "42"},
		{name: "negative", key: -7, encoded: "-7"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			codec := cache.IntKeyCodec{}
			require.Equal(t, testCase.encoded, codec.Encode(testCase.key))
			decoded, err := codec.Decode(testCase.encoded)
			require.NoError(t, err)
			require.Equal(t, testCase.key, decoded)
		})
	}
}

func TestIntKeyCodecDecodeRejectsNonDecimal(t *testing.T) {
	for _, encoded := range []string{"", "not_a_number", "1.5", "0x10"} {
		t.Run(encoded, func(t *testing.T) {
			_, err := cache.IntKeyCodec{}.Decode(encoded)
			require.ErrorContains(t, err, "decode int key")
		})
	}
}
