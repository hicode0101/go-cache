package gocache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLRUEviction(t *testing.T) {
	c := New[int, int](&Config[int, int]{
		Shards:     1, // exact LRU order within one shard
		MaxEntries: 100,
	})
	defer c.Close()

	for i := 0; i < 120; i++ {
		c.Set(i, i)
	}
	if c.Len() != 100 {
		t.Fatalf("Len = %d, want 100", c.Len())
	}
	// oldest 20 keys (0..19) must be evicted
	for i := 0; i < 20; i++ {
		if c.Exists(i) {
			t.Fatalf("key %d should have been evicted", i)
		}
	}
	// newest 100 keys must survive
	for i := 20; i < 120; i++ {
		if !c.Exists(i) {
			t.Fatalf("key %d should have survived", i)
		}
	}
	if st := c.Stats(); st.Evicted != 20 {
		t.Fatalf("evicted = %d, want 20", st.Evicted)
	}
}

func TestLRUTouchOnGet(t *testing.T) {
	c := New[int, int](&Config[int, int]{
		Shards:     1,
		MaxEntries: 10,
	})
	defer c.Close()

	for i := 0; i < 10; i++ {
		c.Set(i, i)
	}
	// touch key 0: it becomes most recently used
	if _, ok := c.Get(0); !ok {
		t.Fatal("warmup get")
	}
	// inserting one more must evict key 1 (the new LRU tail), not 0
	c.Set(10, 10)
	if !c.Exists(0) {
		t.Fatal("key 0 was touched and must survive")
	}
	if c.Exists(1) {
		t.Fatal("key 1 was least recently used and must be evicted")
	}
}

func TestLRUEvictionCallback(t *testing.T) {
	var mu sync.Mutex
	evicted := map[int]RemoveCause{}
	c := New[int, int](&Config[int, int]{
		Shards:     1,
		MaxEntries: 5,
		OnRemoved: func(k int, v int, cause RemoveCause) {
			mu.Lock()
			evicted[k] = cause
			mu.Unlock()
		},
	})
	defer c.Close()

	for i := 0; i < 8; i++ {
		c.Set(i, i)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(evicted) != 3 {
		t.Fatalf("evicted callbacks = %d, want 3", len(evicted))
	}
	for k, cause := range evicted {
		if k > 2 || cause != CauseEvicted {
			t.Fatalf("key %d cause %v", k, cause)
		}
	}
}

func TestEvictionNeverEvictsJustInserted(t *testing.T) {
	c := New[int, int](&Config[int, int]{
		Shards:     1,
		MaxEntries: 1,
		Policy:     PolicyLFU,
	})
	defer c.Close()

	for i := 0; i < 50; i++ {
		c.Set(0, i) // same key over and over
		if c.Len() != 1 {
			t.Fatalf("Len = %d, want 1", c.Len())
		}
		if v, ok := c.Get(0); !ok || v != i {
			t.Fatalf("just-set key vanished at %d", i)
		}
	}
}

func TestLFUEviction(t *testing.T) {
	c := New[int, int](&Config[int, int]{
		Shards:     1,
		MaxEntries: 50,
		Policy:     PolicyLFU,
	})
	defer c.Close()

	for i := 0; i < 50; i++ {
		c.Set(i, i)
	}
	// hammer a few keys so their frequency counters rise
	for round := 0; round < 20; round++ {
		for i := 0; i < 10; i++ {
			c.Get(i)
		}
	}
	// push in 10 more entries; the never-accessed ones should go first
	for i := 50; i < 60; i++ {
		c.Set(i, i)
	}
	if c.Len() > 50 {
		t.Fatalf("Len = %d exceeds max", c.Len())
	}
	survived := 0
	for i := 0; i < 10; i++ {
		if c.Exists(i) {
			survived++
		}
	}
	if survived < 8 {
		t.Fatalf("frequently accessed keys mostly evicted: only %d/10 survived", survived)
	}
}

func TestNonePolicyEvicts(t *testing.T) {
	c := New[int, int](&Config[int, int]{
		Shards:     2,
		MaxEntries: 100,
		Policy:     PolicyNone,
	})
	defer c.Close()

	for i := 0; i < 150; i++ {
		c.Set(i, i)
	}
	if c.Len() > 100 {
		t.Fatalf("Len = %d exceeds max", c.Len())
	}
	if st := c.Stats(); st.Evicted < 50 {
		t.Fatalf("evicted = %d, want >= 50", st.Evicted)
	}
}

func TestMaxMemoryCost(t *testing.T) {
	c := New[int, []byte](&Config[int, []byte]{
		Shards:          1,
		MaxMemory:       10_000,
		CostFunc:        func(k int, v []byte) int64 { return int64(len(v)) + 16 },
		CleanupInterval: time.Hour, // keep janitor out of the picture
	})
	defer c.Close()

	for i := 0; i < 200; i++ {
		c.Set(i, make([]byte, 100)) // 116 bytes each => ~86 fit
	}
	if c.Len() > 87 {
		t.Fatalf("Len = %d exceeds memory budget", c.Len())
	}
	if c.Cost() > 10_000 {
		t.Fatalf("Cost = %d exceeds budget", c.Cost())
	}
	// per-set cost override
	c.Set(9999, make([]byte, 1), WithCost(50))
	if item, ok := c.GetItem(9999); !ok || item.Cost != 50 {
		t.Fatalf("WithCost override failed: %+v", item)
	}
}

func TestEvictionWithTTLItems(t *testing.T) {
	c := New[int, int](&Config[int, int]{
		Shards:     1,
		MaxEntries: 50,
	})
	defer c.Close()

	for i := 0; i < 100; i++ {
		c.SetTTL(i, i, 10*time.Second)
	}
	if c.Len() != 50 {
		t.Fatalf("Len = %d, want 50", c.Len())
	}
	// heap records of evicted entries are stale; a later sweep must not
	// resurrect or misbehave (run a manual sweep)
	for _, s := range c.shards {
		var fires []func()
		s.mu.Lock()
		c.sweepShardLocked(s, &fires)
		s.mu.Unlock()
		runFires(fires)
	}
	if c.Len() != 50 {
		t.Fatalf("Len after sweep = %d, want 50", c.Len())
	}
}

func TestCostConsistency(t *testing.T) {
	c := New[int, int](&Config[int, int]{
		Shards:     4,
		MaxEntries: 1000,
		CostFunc:   func(k int, v int) int64 { return 7 },
	})
	defer c.Close()

	for i := 0; i < 500; i++ {
		c.Set(i, i)
	}
	if c.Cost() != 500*7 {
		t.Fatalf("Cost = %d, want %d", c.Cost(), 500*7)
	}
	for i := 0; i < 100; i++ {
		c.Delete(i)
	}
	if c.Cost() != 400*7 {
		t.Fatalf("Cost after deletes = %d", c.Cost())
	}
	c.Flush()
	if c.Cost() != 0 {
		t.Fatalf("Cost after flush = %d", c.Cost())
	}
}

func TestManyKeysEvictionStress(t *testing.T) {
	c := New[string, int](&Config[string, int]{
		Shards:     16,
		MaxEntries: 10_000,
	})
	defer c.Close()

	for i := 0; i < 50_000; i++ {
		c.Set(fmt.Sprintf("key-%d", i), i)
	}
	if c.Len() > 10_000+16 { // per-shard overshoot tolerance
		t.Fatalf("Len = %d way over max", c.Len())
	}
	t.Logf("final len: %d, evicted: %d", c.Len(), c.Stats().Evicted)
}
