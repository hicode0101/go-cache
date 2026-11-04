package gocache

// Stats is a point-in-time snapshot of cache counters; hit-rate visibility
// is table stakes for a production cache.
type Stats struct {
	// Entries is the number of tracked entries (Len()).
	Entries int
	// Cost is the summed memory weight of tracked entries.
	Cost int64
	// Shards is the number of lock shards.
	Shards int

	// Hits / Misses are read operations that found / did not find a live
	// entry (Get, GetItem, GetOrSet hits).
	Hits   int64
	Misses int64

	// Sets counts new insertions, Updates overwrites of live entries.
	Sets    int64
	Updates int64

	// Deletes counts explicit deletions; Expired entries removed by TTL
	// (lazy or janitor); Evicted entries removed by capacity/memory
	// pressure; Flushed entries removed by Flush.
	Deletes int64
	Expired int64
	Evicted int64
	Flushed int64

	// Loads counts successful loader invocations, LoadErrors failed ones,
	// CoalescedLoads counts callers that joined an in-flight load instead
	// of starting their own (the singleflight savings).
	Loads          int64
	LoadErrors     int64
	CoalescedLoads int64
}

// HitRate returns Hits / (Hits + Misses); 0 when no reads happened.
func (s Stats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// Stats snapshots the cache counters. It briefly locks each shard to
// read its counters; the operation is O(shards) and does not walk
// entries.
func (c *core[K, V]) Stats() Stats {
	st := Stats{
		Shards:         len(c.shards),
		Entries:        c.Len(),
		Cost:           c.Cost(),
		Loads:          c.loads.Load(),
		LoadErrors:     c.loadErrs.Load(),
		CoalescedLoads: c.coalesced.Load(),
		Flushed:        c.flushed.Load(),
	}
	for _, s := range c.shards {
		s.mu.Lock()
		st.Hits += s.st.hits
		st.Misses += s.st.misses
		st.Sets += s.st.sets
		st.Updates += s.st.updates
		st.Deletes += s.st.deletes
		st.Expired += s.st.expired
		st.Evicted += s.st.evicted
		s.mu.Unlock()
	}
	return st
}

// ResetStats zeroes all counters. Entry data is untouched.
func (c *core[K, V]) ResetStats() {
	c.loads.Store(0)
	c.loadErrs.Store(0)
	c.coalesced.Store(0)
	c.flushed.Store(0)
	for _, s := range c.shards {
		s.mu.Lock()
		s.st = shardStats{}
		s.mu.Unlock()
	}
}
