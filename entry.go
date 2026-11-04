package gocache

import (
	"sync/atomic"
	"time"
)

// entry is a single cached item. One entry lives in exactly one shard and
// is linked into that shard's recency list; it is never shared between
// shards, so all plain fields below are guarded by the owning shard's
// mutex. Counters that janitors or readers touch without the shard lock
// use atomics.
//
// Each entry carries no lock of its own: the owning shard's mutex covers
// all its fields. Timestamps are stored as int64 unix-nanos instead of
// 24-byte time.Time values, keeping per-entry overhead small.
type entry[K comparable, V any] struct {
	key   K
	value V

	// expireAt is the absolute unix-nano deadline; 0 = never expires.
	expireAt int64
	// ttl is the configured time-to-live (sliding or absolute); 0 = none.
	// Kept so Item snapshots can report the original Lifespan.
	ttl int64
	// sliding marks ttl as sliding (refreshed on access).
	sliding bool
	// created is the unix-nano insertion timestamp.
	created int64
	// cost is the memory weight charged to the cache's budget.
	cost int64

	// gen is bumped on every replace and removal. Heap entries capture
	// the gen at push time; a mismatch on pop means the heap record is
	// stale and must be discarded without touching the map.
	gen atomic.Uint64

	// accessed / hits are guarded by the shard lock but read via
	// snapshot methods that also hold the lock.
	accessed int64
	hits     int64

	// LFU bookkeeping (only meaningful when Policy == PolicyLFU):
	// freq is an 8-bit logarithmic counter, decayAt is the unix-minute
	// stamp of the last frequency update for lazy decay.
	freq    uint8
	decayAt int32

	// rmCbs hold per-entry removal callbacks registered after insertion.
	// Lazily allocated; nil in the common case so entries stay lean.
	rmCbs []func(key K, value V, cause RemoveCause)

	// Intrusive doubly-linked recency-list links. The list head is the
	// shard's sentinel root; no container/list node allocations.
	prev, next *entry[K, V]
}

// expiredAt reports whether the entry's deadline has passed at now.
func (e *entry[K, V]) expiredAt(now int64) bool {
	return e.expireAt != 0 && now >= e.expireAt
}

// touchRefresh updates access bookkeeping and, for sliding entries,
// extends the deadline. Returns the (possibly new) deadline.
func (e *entry[K, V]) touchRefresh(now int64) int64 {
	e.accessed = now
	e.hits++
	if e.sliding && e.ttl > 0 {
		e.expireAt = now + e.ttl
	}
	return e.expireAt
}

// snapshot converts the entry into an exported, lock-free Item value.
func (e *entry[K, V]) snapshot() Item[K, V] {
	return Item[K, V]{
		Key:         e.key,
		Value:       e.value,
		CreatedOn:   time.Unix(0, e.created),
		AccessedOn:  time.Unix(0, e.accessed),
		AccessCount: e.hits,
		LifeSpan:    time.Duration(e.ttl),
		Sliding:     e.sliding,
		ExpireAt:    time.Unix(0, e.expireAt),
		Cost:        e.cost,
	}
}

// Item is a point-in-time, concurrency-safe snapshot of a cache entry,
// carrying its metadata (CreatedOn, AccessedOn, AccessCount, LifeSpan,
// Key, Data).
type Item[K comparable, V any] struct {
	Key         K
	Value       V
	CreatedOn   time.Time
	AccessedOn  time.Time
	AccessCount int64
	// LifeSpan is the configured TTL; 0 means never expires.
	LifeSpan time.Duration
	// Sliding reports whether LifeSpan is a sliding (access-refreshed)
	// TTL, or an absolute one.
	Sliding bool
	// ExpireAt is the absolute deadline; the zero time means no deadline.
	ExpireAt time.Time
	Cost     int64
}

// addRemoveCallback registers a per-entry removal callback.
func (e *entry[K, V]) addRemoveCallback(fn func(key K, value V, cause RemoveCause)) {
	e.rmCbs = append(e.rmCbs, fn)
}
