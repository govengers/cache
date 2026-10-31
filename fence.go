package cache

import (
	"strings"
	"sync"
)

// readFence keeps a value read from the backend from overwriting a change that was applied
// locally while the read was in flight. It tracks changes per key, and only for keys with reads
// in flight; prefix removals and resets, which touch keys it cannot name, count for every read.
// Every local change and every keep of a read value happens under its mutex, so a change cannot
// slip between the check and the keep.
type readFence[K comparable, V any] struct {
	keyCodec KeyCodec[K]

	mutex         sync.Mutex
	prefixChanges uint64 // prefix removals and resets so far
	readsInFlight map[K]*keyReads
	loadsInFlight map[*loadTracker[K]]struct{}
}

type keyReads struct {
	count   int
	changes uint64
}

type readTicket struct {
	prefixChanges uint64
	keyChanges    uint64
}

// loadTracker collects what changed while a full load was in flight.
type loadTracker[K comparable] struct {
	changedKeys     map[K]struct{}
	changedPrefixes []string
}

func newReadFence[K comparable, V any](keyCodec KeyCodec[K]) *readFence[K, V] {
	return &readFence[K, V]{
		keyCodec:      keyCodec,
		readsInFlight: make(map[K]*keyReads),
		loadsInFlight: make(map[*loadTracker[K]]struct{}),
	}
}

// beginRead registers a read of key; every beginRead is followed by exactly one finishRead.
func (f *readFence[K, V]) beginRead(key K) readTicket {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	reads, found := f.readsInFlight[key]
	if !found {
		reads = &keyReads{}
		f.readsInFlight[key] = reads
	}
	reads.count++
	return readTicket{prefixChanges: f.prefixChanges, keyChanges: reads.changes}
}

// finishRead ends a read of key and calls keep, if any, unless key or a prefix changed since
// beginRead.
func (f *readFence[K, V]) finishRead(key K, ticket readTicket, keep func()) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	reads := f.readsInFlight[key]
	unchanged := ticket.prefixChanges == f.prefixChanges && ticket.keyChanges == reads.changes
	reads.count--
	if reads.count == 0 {
		delete(f.readsInFlight, key)
	}
	if unchanged && keep != nil {
		keep()
	}
}

// changeKey applies a change of key.
func (f *readFence[K, V]) changeKey(key K, apply func()) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if reads, found := f.readsInFlight[key]; found {
		reads.changes++
	}
	for tracker := range f.loadsInFlight {
		tracker.changedKeys[key] = struct{}{}
	}
	apply()
}

// changePrefix applies a change of every key whose encoding starts with prefix; the empty
// prefix stands for every key.
func (f *readFence[K, V]) changePrefix(prefix string, apply func()) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.prefixChanges++
	for tracker := range f.loadsInFlight {
		tracker.changedPrefixes = append(tracker.changedPrefixes, prefix)
	}
	apply()
}

func (f *readFence[K, V]) beginLoad() *loadTracker[K] {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	tracker := &loadTracker[K]{changedKeys: make(map[K]struct{})}
	f.loadsInFlight[tracker] = struct{}{}
	return tracker
}

// finishLoad ends a load and keeps every loaded entry that did not change while it ran.
func (f *readFence[K, V]) finishLoad(tracker *loadTracker[K], entries []Entry[K, V], keep func(key K, value V)) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	delete(f.loadsInFlight, tracker)
	if keep == nil {
		return
	}
	for _, entry := range entries {
		if !f.changedDuringLoad(tracker, entry.Key) {
			keep(entry.Key, entry.Value)
		}
	}
}

func (f *readFence[K, V]) changedDuringLoad(tracker *loadTracker[K], key K) bool {
	if _, changed := tracker.changedKeys[key]; changed {
		return true
	}
	if len(tracker.changedPrefixes) == 0 {
		return false
	}
	encoded := f.keyCodec.Encode(key)
	for _, prefix := range tracker.changedPrefixes {
		if strings.HasPrefix(encoded, prefix) {
			return true
		}
	}
	return false
}
