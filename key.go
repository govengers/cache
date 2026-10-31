package cache

import (
	"fmt"
	"strconv"
)

// KeyCodec converts keys to the strings a cache stores and matches prefixes on. Encode must give
// distinct keys distinct strings, and Decode must turn an encoded key back into the key.
type KeyCodec[K comparable] interface {
	Encode(key K) string
	Decode(encoded string) (K, error)
}

// StringKeyCodec is the KeyCodec for string keys: a key is its own encoding.
type StringKeyCodec struct{}

// Encode returns key unchanged.
func (StringKeyCodec) Encode(key string) string {
	return key
}

// Decode returns encoded unchanged; it never fails.
func (StringKeyCodec) Decode(encoded string) (string, error) {
	return encoded, nil
}

// IntKeyCodec is the KeyCodec for int keys, encoded in decimal.
type IntKeyCodec struct{}

// Encode returns key in decimal.
func (IntKeyCodec) Encode(key int) string {
	return strconv.Itoa(key)
}

// Decode parses a decimal key and fails on anything else.
func (IntKeyCodec) Decode(encoded string) (int, error) {
	key, err := strconv.Atoi(encoded)
	if err != nil {
		return 0, fmt.Errorf("decode int key %q: %w", encoded, err)
	}
	return key, nil
}
