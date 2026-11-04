package gocache

import (
	"sync"
	"sync/atomic"
)

// shard is one lock domain of a Cache. Splitting the key space across
// independent mutexes is the single biggest throughput lever over a
// single table-wide lock: parallel operations on different shards never
// block each other.
type shard[K comparable, V any] struct {
	mu sync.Mutex

	// items is the key -> entry map. Entries are heap-allocated once and
	// updated in place on overwrite, so re-Set of a hot key performs no
	// allocation.
	items map[K]*entry[K, V]

	// root is the sentinel of the intrusive recency list.
	// root.next = most recently used entry, root.prev = least recently
	// used. An empty list has both pointing back at the sentinel itself.
	root entry[K, V]

	// exp is a min-heap of pending expirations ordered by deadline.
	// It may contain stale records (superseded by later writes); those
	// are detected via entry generation counters when popped, avoiding
	// expensive heap surgery on every sliding refresh.
	exp expHeap[K, V]

	// nextExpire mirrors the heap minimum so the janitor can decide when
	// to wake without locking. 0 = no pending expiration.
	nextExpire atomic.Int64

	// count and cost are maintained under mu but summed lock-free by
	// Len/Stats/eviction checks, avoiding cross-shard atomic contention
	// on the hot write path.
	count atomic.Int64
	cost  atomic.Int64

	// st holds this shard's share of the statistics. Written under mu,
	// read under mu by Stats(); no cross-shard cache-line ping-pong.
	st shardStats

	// index is this shard's position in the cache's shard slice; used by
	// the eviction rotator.
	index int
}

// shardStats accumulates per-shard counters.
type shardStats struct {
	hits    int64
	misses  int64
	sets    int64
	updates int64
	deletes int64
	expired int64
	evicted int64
}

func (s *shard[K, V]) init(capHint int) {
	if capHint < 16 {
		capHint = 16
	}
	s.items = make(map[K]*entry[K, V], capHint)
	s.root.next = &s.root
	s.root.prev = &s.root
}

// listPushFront links e at the MRU position.
func (s *shard[K, V]) listPushFront(e *entry[K, V]) {
	e.prev = &s.root
	e.next = s.root.next
	s.root.next.prev = e
	s.root.next = e
}

// listUnlink detaches e from the recency list.
func (s *shard[K, V]) listUnlink(e *entry[K, V]) {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.prev, e.next = nil, nil
}

// listMoveFront marks e as most recently used.
func (s *shard[K, V]) listMoveFront(e *entry[K, V]) {
	// Already at front is the hot case for skewed workloads; skip work.
	if s.root.next == e {
		return
	}
	e.prev.next = e.next
	e.next.prev = e.prev
	s.listPushFront(e)
}

// pushExpiry records a pending expiration for e. The caller must hold
// s.mu and pass the entry's current generation. It reports whether the
// record moved this shard's next deadline EARLIER — the caller must then
// wake the janitor, which may otherwise be napping past the new deadline.
func (s *shard[K, V]) pushExpiry(e *entry[K, V], at int64, gen uint64) bool {
	s.exp.push(expEntry[K, V]{at: at, gen: gen, e: e})
	cur := s.nextExpire.Load()
	if cur == 0 || at < cur {
		s.nextExpire.Store(at)
		return true
	}
	return false
}

// recalcNextExpire re-derives nextExpire from the heap top after sweeps.
func (s *shard[K, V]) recalcNextExpire() {
	if len(s.exp) > 0 {
		s.nextExpire.Store(s.exp[0].at)
	} else {
		s.nextExpire.Store(0)
	}
}

// expHeap is a hand-rolled typed min-heap ordered by deadline. Using a
// concrete generic slice instead of container/heap avoids interface
// boxing and dynamic dispatch on every sift operation.
type expHeap[K comparable, V any] []expEntry[K, V]

type expEntry[K comparable, V any] struct {
	at  int64        // deadline snapshot
	gen uint64       // entry generation at push time
	e   *entry[K, V] // owning entry
}

func (h *expHeap[K, V]) push(x expEntry[K, V]) {
	*h = append(*h, x)
	a := *h
	i := len(a) - 1
	for i > 0 {
		p := (i - 1) / 2
		if a[i].at >= a[p].at {
			break
		}
		a[i], a[p] = a[p], a[i]
		i = p
	}
}

func (h *expHeap[K, V]) popMin() (expEntry[K, V], bool) {
	a := *h
	if len(a) == 0 {
		return expEntry[K, V]{}, false
	}
	top := a[0]
	last := len(a) - 1
	a[0] = a[last]
	*h = a[:last]
	h.down(0)
	return top, true
}

func (h *expHeap[K, V]) peekMin() (expEntry[K, V], bool) {
	a := *h
	if len(a) == 0 {
		return expEntry[K, V]{}, false
	}
	return a[0], true
}

func (h *expHeap[K, V]) down(i int) {
	a := *h
	n := len(a)
	for {
		l := 2*i + 1
		if l >= n {
			return
		}
		m := l
		if r := l + 1; r < n && a[r].at < a[l].at {
			m = r
		}
		if a[m].at >= a[i].at {
			return
		}
		a[i], a[m] = a[m], a[i]
		i = m
	}
}
