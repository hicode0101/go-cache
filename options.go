package gocache

import (
	"context"
	"time"
)

// Policy selects which entry is evicted when a capacity or memory limit is
// exceeded.
type Policy uint8

const (
	// PolicyLRU evicts the least-recently-used entry (default). Every Get
	// and Set moves the entry to the front of the shard's recency list.
	PolicyLRU Policy = iota

	// PolicyLFU evicts entries with the lowest access frequency. Frequency
	// is tracked with a Redis-style 8-bit logarithmic counter with lazy
	// time decay, and victims are chosen by random sampling, so eviction
	// is O(1)-ish regardless of cache size.
	PolicyLFU

	// PolicyNone evicts an effectively random entry. Cheapest option when
	// entries are homogeneous and eviction order does not matter.
	PolicyNone
)

// String implements fmt.Stringer.
func (p Policy) String() string {
	switch p {
	case PolicyLRU:
		return "LRU"
	case PolicyLFU:
		return "LFU"
	case PolicyNone:
		return "NONE"
	default:
		return "UNKNOWN"
	}
}

const (
	// DefaultShards is the number of lock shards used when Config.Shards
	// is not set. It is rounded up to a power of two at construction.
	DefaultShards = 16

	// DefaultCleanupInterval caps how long the background janitor may
	// sleep between sweeps when entries are pending expiration.
	DefaultCleanupInterval = time.Minute

	// lfuSampleSize is how many random entries are inspected when picking
	// an LFU eviction victim (Redis uses 5 by default; 8 gives a better
	// tail at negligible cost).
	lfuSampleSize = 8
)

// Config configures a Cache. A nil *Config means "all defaults".
// Config is generic only because a few fields (Loader, callbacks, cost and
// hash functions) are typed; most fields are plain values.
type Config[K comparable, V any] struct {
	// Shards is the number of lock shards. Higher values reduce lock
	// contention on many-core machines at the cost of a little memory.
	// 0 selects DefaultShards. Values are rounded up to a power of two.
	Shards int

	// Policy selects the eviction strategy. Zero value = PolicyLRU.
	Policy Policy

	// MaxEntries is the maximum number of entries across all shards.
	// 0 (default) disables entry-count based eviction.
	MaxEntries int64

	// MaxMemory is the approximate maximum memory budget, in bytes, as
	// computed from CostFunc. 0 (default) disables memory based eviction.
	MaxMemory int64

	// CostFunc returns the memory weight of an entry. If nil, every entry
	// costs 1, which makes MaxMemory behave like an entry limit.
	CostFunc func(key K, value V) int64

	// HashFunc maps a key to a uint64 used for shard selection. If nil,
	// the runtime's own hash (maphash) is used, which is fast and
	// well-distributed for all comparable types.
	HashFunc func(key K) uint64

	// Loader is the default data loader used by GetOrLoad. Returning a
	// non-nil error aborts caching and propagates the error to all
	// callers.
	Loader func(ctx Ctx, key K, args ...any) (V, error)

	// LoadTTL is the TTL applied to entries inserted by a loader.
	// 0 means loaded entries never expire.
	LoadTTL time.Duration

	// OnAdded fires after a new entry has been inserted. It does NOT fire
	// on overwrites; use OnReplaced for those.
	OnAdded func(key K, value V)

	// OnReplaced fires after an existing entry has been overwritten.
	OnReplaced func(key K, oldValue, newValue V)

	// OnRemoved fires after an entry has been removed for any reason.
	// The cause distinguishes explicit deletes, expirations, evictions
	// and flushes. Callbacks always run outside cache locks, so they may
	// safely call back into the cache.
	OnRemoved func(key K, value V, cause RemoveCause)

	// CleanupInterval caps the janitor's sleep between expiration sweeps.
	// The janitor otherwise wakes exactly when the next entry is due to
	// expire. 0 selects DefaultCleanupInterval.
	CleanupInterval time.Duration

	// ExpiryJitter, when > 0, randomizes each entry's expiration time by
	// up to this duration (added to the TTL). Use it to smooth out mass
	// synchronized expirations of entries written at the same moment.
	ExpiryJitter time.Duration

	// Logger receives debug/warning/error messages. nil = silent.
	Logger Logger
}

// Ctx is an alias keeping Config readable; it equals context.Context.
type Ctx = context.Context

// setParams carries per-call Set modifiers.
type setParams struct {
	ttl     time.Duration
	sliding bool
	cost    int64 // <0 means "derive from CostFunc / default 1"
}

// SetOption customizes a single Set / SetIfAbsent / GetOrSet call.
type SetOption func(*setParams)

// WithTTL sets an absolute time-to-live: the entry expires this long
// after being written, regardless of reads. ttl <= 0 means the entry
// never expires.
func WithTTL(ttl time.Duration) SetOption {
	return func(p *setParams) { p.ttl = ttl }
}

// WithSlidingTTL sets a sliding time-to-live: the entry expires this long
// after the last access (Get / KeepAlive).
func WithSlidingTTL(ttl time.Duration) SetOption {
	return func(p *setParams) { p.ttl, p.sliding = ttl, true }
}

// WithCost overrides the memory cost of this entry, bypassing CostFunc.
func WithCost(cost int64) SetOption {
	return func(p *setParams) { p.cost = cost }
}

// RemoveCause explains why an entry was removed.
type RemoveCause uint8

const (
	// CauseRemoved: the entry was deleted explicitly (Delete /
	// GetAndDelete / CompareAndDelete).
	CauseRemoved RemoveCause = iota

	// CauseExpired: the entry's TTL elapsed (janitor sweep or lazy check).
	CauseExpired

	// CauseEvicted: the entry was evicted to enforce MaxEntries or
	// MaxMemory.
	CauseEvicted

	// CauseFlushed: the whole cache was flushed.
	CauseFlushed
)

// String implements fmt.Stringer.
func (c RemoveCause) String() string {
	switch c {
	case CauseRemoved:
		return "removed"
	case CauseExpired:
		return "expired"
	case CauseEvicted:
		return "evicted"
	case CauseFlushed:
		return "flushed"
	default:
		return "unknown"
	}
}
