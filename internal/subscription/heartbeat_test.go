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

func TestReplicationID(t *testing.T) {
	testCases := []struct {
		name     string
		info     string
		expected string
	}{
		{
			name:     "primary",
			info:     "# Replication\r\nrole:master\r\nconnected_slaves:1\r\nmaster_replid:8f3a\r\nmaster_replid2:0000\r\n",
			expected: "8f3a",
		},
		{
			name:     "secondary replication ID is not taken for the ID",
			info:     "# Replication\r\nmaster_replid2:0000\r\nmaster_replid:8f3a\r\n",
			expected: "8f3a",
		},
		{name: "line feeds only", info: "role:master\nmaster_replid:8f3a\n", expected: "8f3a"},
		{name: "last line without line break", info: "role:master\r\nmaster_replid:8f3a", expected: "8f3a"},
		{name: "no replication ID", info: "# Replication\r\nrole:master\r\n"},
		{name: "empty reply"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, replicationID(testCase.info))
		})
	}
}

func TestHeartbeatPrimaryChanged(t *testing.T) {
	testCases := []struct {
		name              string
		knownPrimaryID    string
		reportedPrimaryID string
		expectedChanged   bool
		expectedKnown     string
	}{
		{name: "first ID is remembered", reportedPrimaryID: "first", expectedKnown: "first"},
		{name: "same ID", knownPrimaryID: "first", reportedPrimaryID: "first", expectedKnown: "first"},
		{name: "other ID", knownPrimaryID: "first", reportedPrimaryID: "second", expectedChanged: true, expectedKnown: "first"},
		{name: "missing ID is ignored", knownPrimaryID: "first", expectedKnown: "first"},
		{name: "missing first ID is ignored"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			heartbeat := &heartbeat{primaryID: testCase.knownPrimaryID}

			changed := heartbeat.primaryChanged(testCase.reportedPrimaryID)

			require.Equal(t, testCase.expectedChanged, changed)
			require.Equal(t, testCase.expectedKnown, heartbeat.primaryID)
		})
	}
}
