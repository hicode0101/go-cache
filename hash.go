package gocache

import "hash/maphash"

// newMaphash returns a hash function for K backed by the Go runtime's own
// hash algorithm (the same one Go maps use internally): fast,
// well-distributed, and randomized per process to resist hash-flooding.
// This avoids interface boxing entirely for concrete key types.
func newMaphash[K comparable]() func(K) uint64 {
	seed := maphash.MakeSeed()
	return func(key K) uint64 { return maphash.Comparable(seed, key) }
}
