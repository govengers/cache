package subscription

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHeartbeatRecord(t *testing.T) {
	testCases := []struct {
		name             string
		payload          string
		expectedLost     bool
		expectedReceived uint64
		expectedRenewed  bool
	}{
		{name: "next number in order", payload: "4", expectedReceived: 4, expectedRenewed: true},
		{name: "duplicate is ignored", payload: "3", expectedReceived: 3},
		{name: "older number is ignored", payload: "2", expectedReceived: 3},
		{name: "unparsable payload is ignored", payload: "beat", expectedReceived: 3},
		{name: "empty payload is ignored", payload: "", expectedReceived: 3},
		{name: "negative payload is ignored", payload: "-5", expectedReceived: 3},
		{name: "skipped number is a lost heartbeat", payload: "5", expectedLost: true, expectedReceived: 5, expectedRenewed: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			before := time.Now().Add(-time.Hour)
			heartbeat := &heartbeat{received: 3, lastReceivedAt: before}

			lost := heartbeat.record(testCase.payload)

			require.Equal(t, testCase.expectedLost, lost)
			require.Equal(t, testCase.expectedReceived, heartbeat.received)
			require.Equal(t, testCase.expectedRenewed, heartbeat.lastReceivedAt.After(before))
		})
	}
}
