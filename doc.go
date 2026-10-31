// Package cache provides typed caches: Local, an in-memory LRU cache with an optional TTL, and
// Synchronized, a local cache in front of a shared Backend that keeps the local copies of several
// processes in step through the backend's events.
//
// The Redis backend lives in the redisadapter subpackage, Redis-based leader election in leader,
// so this package depends on the standard library only.
package cache
