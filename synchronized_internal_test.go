package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSynchronizedApplyIgnoresEventsAfterClose(t *testing.T) {
	local, err := NewLocal[string, string](&LocalOptions[string]{KeyCodec: StringKeyCodec{}})
	require.NoError(t, err)
	synchronized := &Synchronized[string, string]{local: local, fence: newReadFence[string, string](StringKeyCodec{})}
	synchronized.apply(Event[string, string]{Type: EventSet, Key: "kept", Value: "value"})

	synchronized.closed.Store(true)
	for _, event := range []Event[string, string]{
		{Type: EventSet, Key: "ignored", Value: "value"},
		{Type: EventRemove, Key: "kept"},
		{Type: EventRemovePrefix, Prefix: "k"},
		{Type: EventReset},
	} {
		synchronized.apply(event)
	}

	require.Equal(t, []Entry[string, string]{{Key: "kept", Value: "value"}}, local.Entries())
}
