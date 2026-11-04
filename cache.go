package gocache

import (
	"iter"
	"math/rand/v2"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// core holds the actual cache machinery. The exported Cache wraps a core
// pointer so the background janitor goroutine references only the core;
// once the user drops the Cache handle the finalizer can stop the janitor
// and let everything be collected.
type core[K comparable, V any] struct {
	cfg Config[K, V]

	shards []*shard[K, V]
	mask   uint64

	hash    func(K) uint64
	nowNano func() int64

	flights loadGroup[K, V]

	// janitor plumbing
	stopCh  chan struct{}
	wakeCh  chan struct{}
	stopMu  sync.Mutex
	stopped bool

	// global (rare) counters — not touched on the hot path, so shared
	// atomics are fine here.
	loads     atomic.Int64
	loadErrs  atomic.Int64
	coalesced atomic.Int64
	flushed   atomic.Int64
}

// Cache is a concurrency-safe, generic, sharded in-memory cache.
// Create one with New; the zero value is not usable.
type Cache[K comparable, V any] struct {
	*core[K, V]
}

// New creates a cache. A nil cfg selects all defaults, so
// New[string, int](nil) is a fully valid construction.
func New[K comparable, V any](cfg *Config[K, V]) *Cache[K, V] {
	c := &Cache[K, V]{newCore(cfg)}
	// When the caller loses the last reference without ever calling
	// Close, stop the background janitor so it cannot leak.
	runtime.SetFinalizer(c, (*Cache[K, V]).Close)
	return c
}

func newCore[K comparable, V any](cfg *Config[K, V]) *core[K, V] {
	c := &core[K, V]{}
	if cfg != nil {
		c.cfg = *cfg
	}
	ccfg := &c.cfg

	// --- shard setup -------------------------------------------------
	n := ccfg.Shards
	if n <= 0 {
		n = DefaultShards
	}
	n = nextPow2(n)
	c.shards = make([]*shard[K, V], n)
	for i := range c.shards {
		c.shards[i] = &shard[K, V]{index: i}
		c.shards[i].init(16)
	}
	c.mask = uint64(n - 1)

	// --- hashing -----------------------------------------------------
	if ccfg.HashFunc != nil {
		c.hash = ccfg.HashFunc
	} else {
		c.hash = newMaphash[K]()
	}

	// --- defaults ----------------------------------------------------
	if ccfg.CleanupInterval <= 0 {
		ccfg.CleanupInterval = DefaultCleanupInterval
	}
	if ccfg.Logger == nil {
		ccfg.Logger = NopLogger
	}
	if ccfg.CostFunc == nil {
		ccfg.CostFunc = func(K, V) int64 { return 1 }
	}

	c.nowNano = func() int64 { return time.Now().UnixNano() }
	c.stopCh = make(chan struct{})
	c.wakeCh = make(chan struct{}, 1)
	go c.janitorLoop()
	return c
}

// shardFor resolves key to its shard and shard index.
func (c *core[K, V]) shardFor(key K) (*shard[K, V], int) {
	h := c.hash(key)
	idx := int(h & c.mask)
	return c.shards[idx], idx
}

func (c *core[K, V]) now() int64 { return c.nowNano() }

// wake nudges the janitor (non-blocking).
func (c *core[K, V]) wake() {
	select {
	case c.wakeCh <- struct{}{}:
	default:
	}
}

// ---------------------------------------------------------------------------
// Write paths
// ---------------------------------------------------------------------------

// Set inserts or overwrites key. Options may attach a TTL:
//
//	c.Set(k, v, gocache.WithTTL(time.Minute))
//	c.Set(k, v, gocache.WithSlidingTTL(time.Minute)) // sliding semantics
//
// Without options the entry never expires.
func (c *core[K, V]) Set(key K, value V, opts ...SetOption) {
	p := newParams(opts)
	c.setWithParams(key, value, p)
}

// SetTTL is shorthand for Set(key, value, WithTTL(ttl)).
func (c *core[K, V]) SetTTL(key K, value V, ttl time.Duration) {
	c.setWithParams(key, value, &setParams{ttl: ttl, cost: -1})
}

// SetSlidingTTL is shorthand for Set(key, value, WithSlidingTTL(ttl)); the
// entry's deadline renews on every access.
func (c *core[K, V]) SetSlidingTTL(key K, value V, ttl time.Duration) {
	c.setWithParams(key, value, &setParams{ttl: ttl, sliding: true, cost: -1})
}

func newParams(opts []SetOption) *setParams {
	p := &setParams{cost: -1}
	for _, o := range opts {
		o(p)
	}
	return p
}

func (c *core[K, V]) setWithParams(key K, value V, p *setParams) {
	now := c.now()
	s, _ := c.shardFor(key)

	var (
		fires    []func()
		added    bool
		replaced bool
		oldValue V
		needWake bool
	)
	s.mu.Lock()
	if e, ok := s.items[key]; ok {
		if e.expiredAt(now) {
			// Logically expired: treat the overwrite as remove + insert
			// so callbacks and stats observe the expiration.
			c.removeLocked(s, e, CauseExpired, &fires)
		} else {
			// In-place replace: reuse the entry (and its list node) so
			// overwriting a hot key performs no allocation. Bumping the
			// generation invalidates its stale heap records.
			e.gen.Add(1)
			oldValue = e.value
			e.value = value
			e.created = now
			e.accessed = now
			e.hits = 0
			e.freq = 0
			e.decayAt = nowMinute(now)
			e.rmCbs = nil
			oldCost := e.cost
			e.cost = c.costOf(key, value, p)
			s.cost.Add(e.cost - oldCost)
			if c.applyDeadlineLocked(s, e, now, p) {
				needWake = true
			}
			s.listMoveFront(e)
			s.st.updates++
			replaced = true
		}
	}
	if !replaced {
		if c.insertLocked(s, key, value, now, p) {
			needWake = true
		}
		added = true
	}
	s.mu.Unlock()

	if needWake {
		c.wake()
	}

	runFires(fires)
	if added {
		if cb := c.cfg.OnAdded; cb != nil {
			cb(key, value)
		}
	}
	if replaced {
		if cb := c.cfg.OnReplaced; cb != nil {
			cb(key, oldValue, value)
		}
	}
	c.evictIfNeeded(s, key)
}

// SetIfAbsent inserts key only if it is currently absent (or expired).
// Reports whether the insert happened.
func (c *core[K, V]) SetIfAbsent(key K, value V, opts ...SetOption) bool {
	p := newParams(opts)
	now := c.now()
	s, _ := c.shardFor(key)

	var fires []func()
	added := false
	needWake := false
	s.mu.Lock()
	if e, ok := s.items[key]; ok {
		if e.expiredAt(now) {
			c.removeLocked(s, e, CauseExpired, &fires)
		} else {
			s.mu.Unlock()
			runFires(fires)
			return false
		}
	}
	if c.insertLocked(s, key, value, now, p) {
		needWake = true
	}
	added = true
	s.mu.Unlock()

	if needWake {
		c.wake()
	}
	runFires(fires)
	if added {
		if cb := c.cfg.OnAdded; cb != nil {
			cb(key, value)
		}
		c.evictIfNeeded(s, key)
	}
	return added
}

// GetOrSet returns the existing value for key when present (loaded=true);
// otherwise it inserts value (loaded=false). Follows sync.Map.LoadOrStore
// conventions.
func (c *core[K, V]) GetOrSet(key K, value V, opts ...SetOption) (actual V, loaded bool) {
	p := newParams(opts)
	now := c.now()
	s, _ := c.shardFor(key)

	var fires []func()
	needWake := false
	s.mu.Lock()
	if e, ok := s.items[key]; ok {
		if e.expiredAt(now) {
			c.removeLocked(s, e, CauseExpired, &fires)
		} else {
			v := e.value
			e.accessed = now
			e.hits++
			if e.sliding && e.ttl > 0 {
				e.expireAt = now + e.ttl
			}
			c.touchPolicyLocked(s, e, now)
			s.st.hits++
			s.mu.Unlock()
			runFires(fires)
			return v, true
		}
	}
	if c.insertLocked(s, key, value, now, p) {
		needWake = true
	}
	s.mu.Unlock()

	if needWake {
		c.wake()
	}
	runFires(fires)
	if cb := c.cfg.OnAdded; cb != nil {
		cb(key, value)
	}
	c.evictIfNeeded(s, key)
	return value, false
}

// GetAndSet overwrites key and returns the previous value, if any.
func (c *core[K, V]) GetAndSet(key K, value V, opts ...SetOption) (old V, existed bool) {
	p := newParams(opts)
	now := c.now()
	s, _ := c.shardFor(key)

	var fires []func()
	needWake := false
	s.mu.Lock()
	if e, ok := s.items[key]; ok {
		if e.expiredAt(now) {
			c.removeLocked(s, e, CauseExpired, &fires)
		} else {
			old = e.value
			e.gen.Add(1)
			oldCost := e.cost
			e.value = value
			e.created = now
			e.accessed = now
			e.hits = 0
			e.freq = 0
			e.decayAt = nowMinute(now)
			e.rmCbs = nil
			e.cost = c.costOf(key, value, p)
			s.cost.Add(e.cost - oldCost)
			if c.applyDeadlineLocked(s, e, now, p) {
				needWake = true
			}
			s.listMoveFront(e)
			s.st.updates++
			s.mu.Unlock()
			runFires(fires)
			if cb := c.cfg.OnReplaced; cb != nil {
				cb(key, old, value)
			}
			c.evictIfNeeded(s, key)
			return old, true
		}
	}
	if c.insertLocked(s, key, value, now, p) {
		needWake = true
	}
	s.mu.Unlock()

	if needWake {
		c.wake()
	}
	runFires(fires)
	if cb := c.cfg.OnAdded; cb != nil {
		cb(key, value)
	}
	c.evictIfNeeded(s, key)
	return old, false
}

// CompareAndSwap atomically replaces the value when match(oldValue) holds
// and reports whether the swap happened.
func (c *core[K, V]) CompareAndSwap(key K, match func(oldValue V) bool, newValue V, opts ...SetOption) bool {
	p := newParams(opts)
	now := c.now()
	s, _ := c.shardFor(key)
	ok := false
	var cbOld V
	needWake := false

	s.mu.Lock()
	if e, exists := s.items[key]; exists && !e.expiredAt(now) && match(e.value) {
		cbOld = e.value
		e.gen.Add(1)
		oldCost := e.cost
		e.value = newValue
		e.accessed = now
		e.hits++
		e.cost = c.costOf(key, newValue, p)
		s.cost.Add(e.cost - oldCost)
		if c.applyDeadlineLocked(s, e, now, p) {
			needWake = true
		}
		c.touchPolicyLocked(s, e, now)
		s.st.updates++
		ok = true
	}
	s.mu.Unlock()

	if needWake {
		c.wake()
	}
	if ok {
		if cb := c.cfg.OnReplaced; cb != nil {
			cb(key, cbOld, newValue)
		}
		c.evictIfNeeded(s, key)
	}
	return ok
}

// CompareAndDelete atomically deletes key when match(oldValue) holds and
// returns the removed value plus whether the deletion happened.
func (c *core[K, V]) CompareAndDelete(key K, match func(oldValue V) bool) (V, bool) {
	now := c.now()
	s, _ := c.shardFor(key)
	var fires []func()
	var removed V
	ok := false

	s.mu.Lock()
	if e, exists := s.items[key]; exists && !e.expiredAt(now) && match(e.value) {
		removed = e.value
		c.removeLocked(s, e, CauseRemoved, &fires)
		ok = true
	}
	s.mu.Unlock()

	runFires(fires)
	return removed, ok
}

// ---------------------------------------------------------------------------
// Read paths
// ---------------------------------------------------------------------------

// Get returns the value for key and refreshes it: sliding TTLs are
// extended, access counters incremented and LRU order updated.
func (c *core[K, V]) Get(key K) (V, bool) {
	now := c.now()
	s, _ := c.shardFor(key)

	s.mu.Lock()
	e, ok := s.items[key]
	if !ok {
		s.st.misses++
		s.mu.Unlock()
		var zero V
		return zero, false
	}
	if e.expiredAt(now) {
		var fires []func()
		c.removeLocked(s, e, CauseExpired, &fires)
		s.st.misses++
		s.mu.Unlock()
		runFires(fires)
		var zero V
		return zero, false
	}

	v := e.value
	e.touchRefresh(now)
	c.touchPolicyLocked(s, e, now)
	s.st.hits++
	s.mu.Unlock()
	return v, true
}

// Peek returns the value without refreshing anything (no KeepAlive, no
// LRU touch, no stats). Expired entries are treated as absent.
func (c *core[K, V]) Peek(key K) (V, bool) {
	now := c.now()
	s, _ := c.shardFor(key)
	s.mu.Lock()
	e, ok := s.items[key]
	if !ok || e.expiredAt(now) {
		s.mu.Unlock()
		var zero V
		return zero, false
	}
	v := e.value
	s.mu.Unlock()
	return v, true
}

// GetItem behaves like Get but returns the full metadata snapshot.
func (c *core[K, V]) GetItem(key K) (Item[K, V], bool) {
	now := c.now()
	s, _ := c.shardFor(key)
	s.mu.Lock()
	e, ok := s.items[key]
	if !ok {
		s.st.misses++
		s.mu.Unlock()
		return Item[K, V]{}, false
	}
	if e.expiredAt(now) {
		var fires []func()
		c.removeLocked(s, e, CauseExpired, &fires)
		s.st.misses++
		s.mu.Unlock()
		runFires(fires)
		return Item[K, V]{}, false
	}
	e.touchRefresh(now)
	c.touchPolicyLocked(s, e, now)
	item := e.snapshot()
	s.st.hits++
	s.mu.Unlock()
	return item, true
}

// Exists reports whether key is present, without refreshing it.
func (c *core[K, V]) Exists(key K) bool {
	now := c.now()
	s, _ := c.shardFor(key)
	s.mu.Lock()
	e, ok := s.items[key]
	if !ok {
		s.mu.Unlock()
		return false
	}
	if e.expiredAt(now) {
		var fires []func()
		c.removeLocked(s, e, CauseExpired, &fires)
		s.mu.Unlock()
		runFires(fires)
		return false
	}
	s.mu.Unlock()
	return true
}

// KeepAlive refreshes key's sliding deadline and access bookkeeping
// without returning the value. Reports whether the key is still alive.
func (c *core[K, V]) KeepAlive(key K) bool {
	now := c.now()
	s, _ := c.shardFor(key)
	s.mu.Lock()
	e, ok := s.items[key]
	if !ok {
		s.mu.Unlock()
		return false
	}
	if e.expiredAt(now) {
		var fires []func()
		c.removeLocked(s, e, CauseExpired, &fires)
		s.mu.Unlock()
		runFires(fires)
		return false
	}
	e.touchRefresh(now)
	c.touchPolicyLocked(s, e, now)
	s.mu.Unlock()
	return true
}

// ---------------------------------------------------------------------------
// Delete / lifecycle
// ---------------------------------------------------------------------------

// Delete removes key and reports whether it existed. The removed value is
// available via GetAndDelete.
func (c *core[K, V]) Delete(key K) bool {
	now := c.now()
	s, _ := c.shardFor(key)
	var fires []func()
	s.mu.Lock()
	e, ok := s.items[key]
	if !ok {
		s.mu.Unlock()
		return false
	}
	cause := CauseRemoved
	if e.expiredAt(now) {
		cause = CauseExpired
	}
	c.removeLocked(s, e, cause, &fires)
	s.mu.Unlock()
	runFires(fires)
	return cause == CauseRemoved
}

// GetAndDelete removes key and returns its previous value.
func (c *core[K, V]) GetAndDelete(key K) (V, bool) {
	now := c.now()
	s, _ := c.shardFor(key)
	var fires []func()
	var v V
	ok := false

	s.mu.Lock()
	if e, exists := s.items[key]; exists {
		cause := CauseRemoved
		if e.expiredAt(now) {
			cause = CauseExpired
		} else {
			v = e.value
		}
		c.removeLocked(s, e, cause, &fires)
		ok = cause == CauseRemoved
	}
	s.mu.Unlock()
	runFires(fires)
	return v, ok
}

// SetRemoveCallback attaches a per-entry removal callback to an existing
// key after insertion.
func (c *core[K, V]) SetRemoveCallback(key K, fn func(key K, value V, cause RemoveCause)) bool {
	now := c.now()
	s, _ := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.items[key]
	if !ok || e.expiredAt(now) {
		return false
	}
	e.addRemoveCallback(fn)
	return true
}

// Flush removes every entry, firing removal callbacks with CauseFlushed
// so resource-cleanup hooks can run.
func (c *core[K, V]) Flush() {
	c.cfg.Logger.Debugf("flushing cache")
	for _, s := range c.shards {
		var fires []func()
		var flushed int64
		s.mu.Lock()
		if n := len(s.items); n > 0 {
			for _, e := range s.items {
				key, val := e.key, e.value
				cbs := e.rmCbs
				fires = append(fires, func() {
					for _, cb := range cbs {
						cb(key, val, CauseFlushed)
					}
					if cb := c.cfg.OnRemoved; cb != nil {
						cb(key, val, CauseFlushed)
					}
				})
			}
			s.items = make(map[K]*entry[K, V])
			s.root.next = &s.root
			s.root.prev = &s.root
			s.exp = s.exp[:0]
			s.nextExpire.Store(0)
			s.count.Store(0)
			s.cost.Store(0)
			s.st.deletes += int64(n)
			flushed = int64(n)
		}
		s.mu.Unlock()
		runFires(fires)
		if flushed > 0 {
			c.flushed.Add(flushed)
			flushed = 0
		}
	}
}

// Close stops the background janitor. The cache remains usable afterwards
// (expired entries are still removed lazily on read); the janitor simply
// no longer sweeps proactively. Close is idempotent.
func (c *core[K, V]) Close() {
	c.stopMu.Lock()
	defer c.stopMu.Unlock()
	if c.stopped {
		return
	}
	c.stopped = true
	close(c.stopCh)
}

// ---------------------------------------------------------------------------
// Introspection & iteration
// ---------------------------------------------------------------------------

// Len returns the number of entries currently tracked. Logically-expired
// but not-yet-swept entries are counted (standard behavior for sharded
// caches; reads still treat them as absent).
func (c *core[K, V]) Len() int {
	var n int64
	for _, s := range c.shards {
		n += s.count.Load()
	}
	return int(n)
}

// Count is an alias for Len.
func (c *core[K, V]) Count() int { return c.Len() }

// Cost returns the summed memory weight of all entries.
func (c *core[K, V]) Cost() int64 {
	var n int64
	for _, s := range c.shards {
		n += s.cost.Load()
	}
	return n
}

// Keys returns all live (non-expired) keys.
func (c *core[K, V]) Keys() []K {
	now := c.now()
	keys := make([]K, 0, c.Len())
	for _, s := range c.shards {
		s.mu.Lock()
		for k, e := range s.items {
			if !e.expiredAt(now) {
				keys = append(keys, k)
			}
		}
		s.mu.Unlock()
	}
	return keys
}

// ForEach calls fn for every live entry until fn returns false. fn runs
// outside shard locks, so it may safely call back into the cache.
func (c *core[K, V]) ForEach(fn func(key K, value V) bool) {
	for _, s := range c.shards {
		now := c.now()
		type kv struct {
			k K
			v V
		}
		s.mu.Lock()
		pairs := make([]kv, 0, len(s.items))
		for k, e := range s.items {
			if !e.expiredAt(now) {
				pairs = append(pairs, kv{k, e.value})
			}
		}
		s.mu.Unlock()
		for _, p := range pairs {
			if !fn(p.k, p.v) {
				return
			}
		}
	}
}

// All returns a range-over-func iterator over key/value pairs:
//
//	for k, v := range cache.All() { ... }
//
// Iteration is a weakly-consistent snapshot taken per shard.
func (c *core[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		c.ForEach(func(k K, v V) bool { return yield(k, v) })
	}
}

// AllItems iterates full metadata snapshots.
func (c *core[K, V]) AllItems() iter.Seq2[K, Item[K, V]] {
	return func(yield func(K, Item[K, V]) bool) {
		for _, s := range c.shards {
			now := c.now()
			s.mu.Lock()
			items := make([]Item[K, V], 0, len(s.items))
			for _, e := range s.items {
				if !e.expiredAt(now) {
					items = append(items, e.snapshot())
				}
			}
			s.mu.Unlock()
			for _, it := range items {
				if !yield(it.Key, it) {
					return
				}
			}
		}
	}
}

// MostAccessed returns up to n entries sorted by access count, descending.
// Pass n < 0 for all entries.
// Items are snapshots taken under shard locks.
func (c *core[K, V]) MostAccessed(n int) []Item[K, V] {
	now := c.now()
	all := make([]Item[K, V], 0, c.Len())
	for _, s := range c.shards {
		s.mu.Lock()
		for _, e := range s.items {
			if !e.expiredAt(now) {
				all = append(all, e.snapshot())
			}
		}
		s.mu.Unlock()
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].AccessCount > all[j].AccessCount
	})
	if n >= 0 && n < len(all) {
		all = all[:n]
	}
	return all
}

// ---------------------------------------------------------------------------
// Internals shared by write paths
// ---------------------------------------------------------------------------

// insertLocked creates and links a new entry. Caller holds s.mu.
// Reports whether the janitor must be woken: a deadline earlier than the
// currently scheduled sweep was registered.
func (c *core[K, V]) insertLocked(s *shard[K, V], key K, value V, now int64, p *setParams) bool {
	cost := c.costOf(key, value, p)
	e := &entry[K, V]{
		key:      key,
		value:    value,
		created:  now,
		accessed: now,
		cost:     cost,
		decayAt:  nowMinute(now),
	}
	needWake := false
	if p.ttl > 0 {
		deadline := now + int64(p.ttl)
		if c.cfg.ExpiryJitter > 0 {
			deadline += rand.Int64N(int64(c.cfg.ExpiryJitter))
		}
		e.expireAt = deadline
		e.ttl = int64(p.ttl)
		e.sliding = p.sliding
		needWake = s.pushExpiry(e, deadline, e.gen.Load())
	}
	s.items[key] = e
	s.listPushFront(e)
	s.count.Add(1)
	s.cost.Add(cost)
	s.st.sets++
	return needWake
}

// removeLocked unlinks and deletes e, recording callbacks to fire after
// the shard lock is released. Caller holds s.mu.
func (c *core[K, V]) removeLocked(s *shard[K, V], e *entry[K, V], cause RemoveCause, fires *[]func()) {
	delete(s.items, e.key)
	if e.prev != nil {
		s.listUnlink(e)
	}
	e.gen.Add(1)
	s.count.Add(-1)
	s.cost.Add(-e.cost)
	switch cause {
	case CauseExpired:
		s.st.expired++
	case CauseEvicted:
		s.st.evicted++
	default:
		s.st.deletes++
	}
	key, val := e.key, e.value
	cbs := e.rmCbs
	own := c.cfg.OnRemoved
	if len(cbs) > 0 || own != nil {
		*fires = append(*fires, func() {
			for _, cb := range cbs {
				cb(key, val, cause)
			}
			if own != nil {
				own(key, val, cause)
			}
		})
	}
}

// applyDeadlineLocked recomputes an existing entry's deadline from params
// and re-registers it in the expiry heap. Caller holds s.mu. Reports
// whether the janitor must be woken.
func (c *core[K, V]) applyDeadlineLocked(s *shard[K, V], e *entry[K, V], now int64, p *setParams) bool {
	if p.ttl > 0 {
		deadline := now + int64(p.ttl)
		if c.cfg.ExpiryJitter > 0 {
			deadline += rand.Int64N(int64(c.cfg.ExpiryJitter))
		}
		e.expireAt = deadline
		e.ttl = int64(p.ttl)
		e.sliding = p.sliding
		return s.pushExpiry(e, deadline, e.gen.Load())
	}
	e.expireAt = 0
	e.ttl = 0
	e.sliding = false
	return false
}

func (c *core[K, V]) costOf(key K, value V, p *setParams) int64 {
	if p != nil && p.cost >= 0 {
		return p.cost
	}
	return c.cfg.CostFunc(key, value)
}

// touchPolicyLocked applies the configured policy's read-side bookkeeping.
func (c *core[K, V]) touchPolicyLocked(s *shard[K, V], e *entry[K, V], now int64) {
	switch c.cfg.Policy {
	case PolicyLRU:
		s.listMoveFront(e)
	case PolicyLFU:
		s.listMoveFront(e)
		c.lfuTouchLocked(e, now)
	default: // PolicyNone
	}
}

// lfuTouchLocked advances the 8-bit logarithmic frequency counter with
// lazy decay (Redis-style): first apply minutes elapsed as right-shifts,
// then grow the counter logarithmically.
func (c *core[K, V]) lfuTouchLocked(e *entry[K, V], now int64) {
	minNow := nowMinute(now)
	if elapsed := minNow - e.decayAt; elapsed > 0 {
		shift := elapsed
		if shift > 8 {
			shift = 8
		}
		e.freq >>= uint(shift)
		e.decayAt = minNow
	}
	if e.freq == 0 {
		e.freq = 1
	} else if e.freq < 255 {
		// logarithmic increment: the larger the counter, the smaller
		// the growth step
		e.freq += uint8(255 / (int(e.freq)*8 + 1))
	}
}

// evictIfNeeded enforces MaxEntries / MaxMemory after an insert into
// shard index idx.
func (c *core[K, V]) evictIfNeeded(s *shard[K, V], justKey K) {
	if c.cfg.MaxEntries <= 0 && c.cfg.MaxMemory <= 0 {
		return
	}
	for i := 0; i < 1_000_000; i++ { // hard bound; each pass evicts >= 1
		overEntries := c.cfg.MaxEntries > 0 && c.Len() > int(c.cfg.MaxEntries)
		overMemory := c.cfg.MaxMemory > 0 && c.Cost() > c.cfg.MaxMemory
		if !overEntries && !overMemory {
			return
		}
		if !c.evictOne(s, justKey) {
			return
		}
	}
}

// evictOne removes a single victim, starting the search at shard s and
// rotating through the others. Reports whether anything was evicted.
func (c *core[K, V]) evictOne(s *shard[K, V], justKey K) bool {
	for i := 0; i < len(c.shards); i++ {
		sh := c.shards[(s.index+i)%len(c.shards)]
		var fires []func()
		sh.mu.Lock()
		victim := c.pickVictimLocked(sh, justKey)
		if victim != nil {
			c.removeLocked(sh, victim, CauseEvicted, &fires)
		}
		sh.mu.Unlock()
		if victim != nil {
			runFires(fires)
			return true
		}
	}
	return false
}

// pickVictimLocked chooses the entry to evict according to Policy.
// justKey (the entry being inserted right now) is never chosen.
func (c *core[K, V]) pickVictimLocked(s *shard[K, V], justKey K) *entry[K, V] {
	if len(s.items) == 0 {
		return nil
	}
	switch c.cfg.Policy {
	case PolicyLRU:
		// least recently used = list tail; justKey can appear at most
		// once in the list, so skipping it is O(1) in practice.
		for e := s.root.prev; e != &s.root; e = e.prev {
			if e.key != justKey {
				return e
			}
		}
		return nil
	case PolicyLFU:
		var victim *entry[K, V]
		i := 0
		for _, e := range s.items {
			if e.key == justKey {
				continue
			}
			if victim == nil || e.freq < victim.freq ||
				(e.freq == victim.freq && e.accessed < victim.accessed) {
				victim = e
			}
			i++
			if i >= lfuSampleSize {
				break
			}
		}
		return victim
	default: // PolicyNone — effectively random via map order
		for _, e := range s.items {
			if e.key != justKey {
				return e
			}
		}
		return nil
	}
}

func runFires(fires []func()) {
	for _, f := range fires {
		f()
	}
}

func nowMinute(nanos int64) int32 {
	return int32(nanos / int64(time.Minute))
}

func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}
