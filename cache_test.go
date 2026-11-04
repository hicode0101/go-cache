package gocache

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testKey   = "testkey"
	testValue = "testvalue"
)

func TestSetGetBasic(t *testing.T) {
	c := New[string, string](nil)
	defer c.Close()

	c.Set(testKey, testValue)
	v, ok := c.Get(testKey)
	if !ok || v != testValue {
		t.Fatalf("Get = %q, %v; want %q, true", v, ok, testValue)
	}

	if _, ok := c.Get("missing"); ok {
		t.Fatal("missing key should not be found")
	}
}

func TestSetOverwrite(t *testing.T) {
	var (
		mu       sync.Mutex
		added    []string
		replaced [][2]string
	)
	c := New[string, int](&Config[string, int]{
		OnAdded: func(k string, v int) { mu.Lock(); added = append(added, k); mu.Unlock() },
		OnReplaced: func(k string, oldV, newV int) {
			mu.Lock()
			replaced = append(replaced, [2]string{k + ":" + itoa(oldV), k + ":" + itoa(newV)})
			mu.Unlock()
		},
	})
	defer c.Close()

	c.Set("k", 1)
	c.Set("k", 2)
	c.Set("k", 3)

	if v, _ := c.Get("k"); v != 3 {
		t.Fatalf("value = %d, want 3", v)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(added) != 1 {
		t.Fatalf("OnAdded fired %d times, want 1", len(added))
	}
	if len(replaced) != 2 {
		t.Fatalf("OnReplaced fired %d times, want 2", len(replaced))
	}
	if replaced[0] != [2]string{"k:1", "k:2"} || replaced[1] != [2]string{"k:2", "k:3"} {
		t.Fatalf("replace sequence wrong: %v", replaced)
	}
}

func TestDeleteAndGetAndDelete(t *testing.T) {
	c := New[string, string](nil)
	defer c.Close()

	c.Set(testKey, testValue)
	if !c.Delete(testKey) {
		t.Fatal("Delete should report true for existing key")
	}
	if c.Delete(testKey) {
		t.Fatal("Delete should report false for missing key")
	}
	if _, ok := c.Get(testKey); ok {
		t.Fatal("deleted key should be gone")
	}

	c.Set(testKey, "v2")
	v, ok := c.GetAndDelete(testKey)
	if !ok || v != "v2" {
		t.Fatalf("GetAndDelete = %q, %v", v, ok)
	}
	if _, ok := c.GetAndDelete(testKey); ok {
		t.Fatal("second GetAndDelete should miss")
	}
}

func TestExistsAndLen(t *testing.T) {
	c := New[int, int](nil)
	defer c.Close()

	for i := 0; i < 100; i++ {
		c.Set(i, i*i)
	}
	if c.Len() != 100 || c.Count() != 100 {
		t.Fatalf("Len = %d", c.Len())
	}
	for i := 0; i < 100; i++ {
		if !c.Exists(i) {
			t.Fatalf("key %d should exist", i)
		}
	}
	if c.Exists(100) {
		t.Fatal("key 100 should not exist")
	}
}

func TestSetIfAbsent(t *testing.T) {
	c := New[string, int](nil)
	defer c.Close()

	if !c.SetIfAbsent("a", 1) {
		t.Fatal("first SetIfAbsent should succeed")
	}
	if c.SetIfAbsent("a", 2) {
		t.Fatal("second SetIfAbsent should fail")
	}
	if v, _ := c.Get("a"); v != 1 {
		t.Fatalf("value = %d, want 1 (unchanged)", v)
	}

	// expired entry is replaceable via SetIfAbsent
	c.SetTTL("b", 1, 100*time.Millisecond)
	time.Sleep(250 * time.Millisecond)
	if !c.SetIfAbsent("b", 2) {
		t.Fatal("SetIfAbsent should succeed over expired entry")
	}
}

func TestSetIfAbsentConcurrent(t *testing.T) {
	c := New[int, int](nil)
	defer c.Close()

	const goroutines = 16
	var added atomic.Int32
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if c.SetIfAbsent(i, i+id) {
					added.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()

	if int(added.Load()) != 200 {
		t.Fatalf("added = %d, want exactly 200", added.Load())
	}
	if c.Len() != 200 {
		t.Fatalf("len = %d, want 200", c.Len())
	}
}

func TestGetOrSet(t *testing.T) {
	c := New[string, int](nil)
	defer c.Close()

	v, loaded := c.GetOrSet("a", 1)
	if loaded || v != 1 {
		t.Fatalf("first GetOrSet = (%d, %v), want (1, false)", v, loaded)
	}
	v, loaded = c.GetOrSet("a", 99)
	if !loaded || v != 1 {
		t.Fatalf("second GetOrSet = (%d, %v), want (1, true)", v, loaded)
	}
}

func TestGetAndSet(t *testing.T) {
	c := New[string, int](nil)
	defer c.Close()

	old, existed := c.GetAndSet("a", 1)
	if existed || old != 0 {
		t.Fatalf("first GetAndSet = (%d, %v)", old, existed)
	}
	old, existed = c.GetAndSet("a", 2)
	if !existed || old != 1 {
		t.Fatalf("second GetAndSet = (%d, %v), want (1, true)", old, existed)
	}
}

func TestCompareAndSwapAndDelete(t *testing.T) {
	c := New[string, int](nil)
	defer c.Close()

	c.Set("a", 10)
	if c.CompareAndSwap("a", func(old int) bool { return old == 99 }, 20) {
		t.Fatal("CAS with wrong predicate must fail")
	}
	if !c.CompareAndSwap("a", func(old int) bool { return old == 10 }, 20) {
		t.Fatal("CAS with right predicate must succeed")
	}
	if v, _ := c.Get("a"); v != 20 {
		t.Fatalf("value after CAS = %d, want 20", v)
	}

	if _, ok := c.CompareAndDelete("a", func(old int) bool { return old == 1 }); ok {
		t.Fatal("CompareAndDelete with wrong predicate must fail")
	}
	removed, ok := c.CompareAndDelete("a", func(old int) bool { return old == 20 })
	if !ok || removed != 20 {
		t.Fatalf("CompareAndDelete = (%d, %v)", removed, ok)
	}
	if c.Exists("a") {
		t.Fatal("key should be deleted")
	}
}

func TestGetItemMetadata(t *testing.T) {
	c := New[string, string](nil)
	defer c.Close()

	before := time.Now()
	c.SetTTL(testKey, testValue, time.Minute)

	item, ok := c.GetItem(testKey)
	if !ok {
		t.Fatal("GetItem should find the key")
	}
	if item.Key != testKey || item.Value != testValue {
		t.Fatalf("item = %+v", item)
	}
	if item.LifeSpan != time.Minute {
		t.Fatalf("LifeSpan = %v, want 1m", item.LifeSpan)
	}
	if item.Sliding {
		t.Fatal("WithTTL must not be sliding")
	}
	if item.CreatedOn.Before(before.Add(-time.Second)) {
		t.Fatalf("CreatedOn = %v, too old", item.CreatedOn)
	}
	if item.AccessCount != 1 {
		t.Fatalf("AccessCount = %d, want 1", item.AccessCount)
	}
	d, _ := c.ExpireIn(testKey)
	if d <= 0 || d > time.Minute {
		t.Fatalf("ExpireIn = %v, want (0, 1m]", d)
	}
	// never-expiring key
	c.Set("forever", "x")
	if _, ok := c.ExpireIn("forever"); ok {
		t.Fatal("never-expiring key must report no deadline")
	}
}

func TestPeekDoesNotRefresh(t *testing.T) {
	c := New[string, int](nil)
	defer c.Close()

	// Peek must never extend a sliding TTL: the entry dies at exactly
	// ttl after its last real access, no matter how often we peek.
	c.SetSlidingTTL("b", 1, 300*time.Millisecond)
	time.Sleep(120 * time.Millisecond)
	if _, ok := c.Peek("b"); !ok {
		t.Fatal("b should still be alive before its TTL")
	}
	time.Sleep(300 * time.Millisecond) // total 420ms > ttl
	if _, ok := c.Peek("b"); ok {
		t.Fatal("Peek refreshed the sliding TTL; it must be read-only")
	}

	// contrast: Get DOES refresh (KeepAlive semantics)
	c.SetSlidingTTL("g", 1, 300*time.Millisecond)
	for i := 0; i < 3; i++ {
		time.Sleep(60 * time.Millisecond)
		if _, ok := c.Get("g"); !ok {
			t.Fatalf("Get failed to keep g alive at iter %d", i)
		}
	}
}

func TestKeepAlive(t *testing.T) {
	c := New[string, int](nil)
	defer c.Close()

	c.SetSlidingTTL("a", 1, 400*time.Millisecond)
	time.Sleep(150 * time.Millisecond)
	if !c.KeepAlive("a") {
		t.Fatal("KeepAlive should find live key")
	}
	time.Sleep(150 * time.Millisecond)
	if !c.Exists("a") {
		t.Fatal("key should survive past original TTL after KeepAlive")
	}
	time.Sleep(600 * time.Millisecond)
	if c.KeepAlive("a") {
		t.Fatal("KeepAlive on expired key must fail")
	}
	if c.KeepAlive("never-there") {
		t.Fatal("KeepAlive on missing key must fail")
	}
}

func TestMostAccessed(t *testing.T) {
	c := New[int, int](nil)
	defer c.Close()

	for i := 0; i < 50; i++ {
		c.Set(i, i)
	}
	for i := 0; i < 50; i++ {
		for j := 0; j < i; j++ {
			c.Get(i)
		}
	}
	ma := c.MostAccessed(5)
	if len(ma) != 5 {
		t.Fatalf("MostAccessed(5) len = %d", len(ma))
	}
	for i, item := range ma {
		if want := 49 - i; item.Key != want {
			t.Fatalf("rank %d = key %d, want %d", i, item.Key, want)
		}
	}
	if got := len(c.MostAccessed(-1)); got != 50 {
		t.Fatalf("MostAccessed(-1) len = %d, want 50", got)
	}
}

func TestAccessCountSemantics(t *testing.T) {
	c := New[string, int](nil)
	defer c.Close()

	// AllItems snapshots do not count as accesses, so they can read the
	// counter without perturbing it (GetItem would increment it).
	countOf := func(key string) int64 {
		for k, it := range c.AllItems() {
			if k == key {
				return it.AccessCount
			}
		}
		return -1
	}

	c.Set("k", 1)
	if n := countOf("k"); n != 0 {
		t.Fatalf("insert must not count as access, got %d", n)
	}
	c.Get("k")
	c.Get("k")
	if n := countOf("k"); n != 2 {
		t.Fatalf("after 2 Gets = %d, want 2", n)
	}
	c.Peek("k")
	c.Peek("k")
	c.Exists("k")
	if n := countOf("k"); n != 2 {
		t.Fatalf("Peek/Exists must not count, got %d", n)
	}
	c.KeepAlive("k")
	if n := countOf("k"); n != 3 {
		t.Fatalf("KeepAlive must count, got %d", n)
	}
	if _, loaded := c.GetOrSet("k", 2); !loaded {
		t.Fatal("GetOrSet should hit")
	}
	if n := countOf("k"); n != 4 {
		t.Fatalf("GetOrSet hit must count, got %d", n)
	}
	// GetOrSet hit did NOT replace the value (sync.Map semantics): still 1
	if !c.CompareAndSwap("k", func(old int) bool { return old == 1 }, 3) {
		t.Fatal("CAS should succeed")
	}
	if n := countOf("k"); n != 5 {
		t.Fatalf("successful CAS must count, got %d", n)
	}
	if got := c.GetMany("k"); len(got) != 1 {
		t.Fatal("GetMany should find k")
	}
	if n := countOf("k"); n != 6 {
		t.Fatalf("GetMany hit must count, got %d", n)
	}
	c.Set("k", 9) // overwrite resets the counter
	if n := countOf("k"); n != 0 {
		t.Fatalf("overwrite must reset AccessCount, got %d", n)
	}
	if item, ok := c.GetItem("k"); !ok || item.AccessCount != 1 {
		t.Fatalf("GetItem after reset = %d, want 1 (its own access)", item.AccessCount)
	}
}

func TestAccessCountConcurrent(t *testing.T) {
	c := New[int, int](nil)
	defer c.Close()

	const (
		goroutines = 16
		perG       = 400
	)
	keys := []int{0, 1, 2, 3}
	for _, k := range keys {
		c.Set(k, k)
	}

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				k := keys[(id+i)%len(keys)]
				// every branch counts exactly one access
				switch i % 4 {
				case 0:
					c.Get(k)
				case 1:
					c.GetItem(k)
				case 2:
					c.KeepAlive(k)
				case 3:
					c.GetOrSet(k, k)
				}
			}
		}(g)
	}
	wg.Wait()

	// distribution is deterministic: each key is touched exactly
	// goroutines*perG/len(keys) times; a single lost update fails this.
	wantPerKey := int64(goroutines * perG / len(keys))
	got := make(map[int]int64)
	var total int64
	for _, it := range c.MostAccessed(-1) {
		got[it.Key] = it.AccessCount
		total += it.AccessCount
	}
	if want := int64(goroutines * perG); total != want {
		t.Fatalf("total AccessCount = %d, want %d (lost updates?)", total, want)
	}
	for _, k := range keys {
		if got[k] != wantPerKey {
			t.Fatalf("AccessCount[%d] = %d, want %d", k, got[k], wantPerKey)
		}
	}
}

func TestForEachAndAll(t *testing.T) {
	c := New[int, int](nil)
	defer c.Close()

	for i := 0; i < 100; i++ {
		c.Set(i, i*2)
	}
	seen := map[int]int{}
	c.ForEach(func(k, v int) bool {
		seen[k] = v
		return true
	})
	if len(seen) != 100 {
		t.Fatalf("ForEach visited %d entries", len(seen))
	}
	for k, v := range seen {
		if v != k*2 {
			t.Fatalf("entry %d = %d", k, v)
		}
	}

	// early stop
	count := 0
	c.ForEach(func(k, v int) bool {
		count++
		return count < 10
	})
	if count != 10 {
		t.Fatalf("early stop after %d, want 10", count)
	}

	// range-over-func
	total := 0
	for k, v := range c.All() {
		total += k + v
	}
	want := 0
	for i := 0; i < 100; i++ {
		want += i + i*2
	}
	if total != want {
		t.Fatalf("All() sum = %d, want %d", total, want)
	}

	items := map[int]Item[int, int]{}
	for k, it := range c.AllItems() {
		items[k] = it
	}
	if len(items) != 100 {
		t.Fatalf("AllItems visited %d", len(items))
	}
}

func TestBatchOps(t *testing.T) {
	c := New[int, int](nil)
	defer c.Close()

	pairs := map[int]int{}
	for i := 0; i < 50; i++ {
		pairs[i] = i * 3
	}
	c.SetMany(pairs, WithTTL(time.Minute))
	if c.Len() != 50 {
		t.Fatalf("Len after SetMany = %d", c.Len())
	}

	got := c.GetMany(1, 2, 999, 3)
	if len(got) != 3 {
		t.Fatalf("GetMany found %d, want 3", len(got))
	}
	if got[2] != 6 {
		t.Fatalf("GetMany[2] = %d", got[2])
	}

	if n := c.DeleteMany(1, 2, 999); n != 2 {
		t.Fatalf("DeleteMany removed %d, want 2", n)
	}
	if c.Len() != 48 {
		t.Fatalf("Len = %d, want 48", c.Len())
	}
}

func TestFlushCallbacks(t *testing.T) {
	var mu sync.Mutex
	flushedKeys := map[string]RemoveCause{}
	c := New[string, int](&Config[string, int]{
		OnRemoved: func(k string, v int, cause RemoveCause) {
			mu.Lock()
			flushedKeys[k] = cause
			mu.Unlock()
		},
	})
	defer c.Close()

	for i := 0; i < 10; i++ {
		c.Set(strconv.Itoa(i), i)
	}
	c.Flush()

	mu.Lock()
	defer mu.Unlock()
	if len(flushedKeys) != 10 {
		t.Fatalf("flush fired %d callbacks, want 10", len(flushedKeys))
	}
	for k, cause := range flushedKeys {
		if cause != CauseFlushed {
			t.Fatalf("key %s cause = %v", k, cause)
		}
	}
	if c.Len() != 0 {
		t.Fatalf("Len after flush = %d", c.Len())
	}
	if st := c.Stats(); st.Flushed != 10 {
		t.Fatalf("stats flushed = %d", st.Flushed)
	}
}

func TestPerEntryRemoveCallback(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]RemoveCause{}
	c := New[string, int](nil)
	defer c.Close()

	c.Set("a", 1)
	c.Set("b", 2)
	c.SetRemoveCallback("a", func(k string, v int, cause RemoveCause) {
		mu.Lock()
		seen[k] = cause
		mu.Unlock()
	})

	c.Delete("a")
	mu.Lock()
	if seen["a"] != CauseRemoved {
		t.Fatalf("per-entry callback cause = %v", seen["a"])
	}
	mu.Unlock()

	// entry callback fires on expiry too
	c.SetTTL("c", 3, 200*time.Millisecond)
	c.SetRemoveCallback("c", func(k string, v int, cause RemoveCause) {
		mu.Lock()
		seen[k] = cause
		mu.Unlock()
	})
	time.Sleep(350 * time.Millisecond)
	c.Get("c") // triggers lazy removal with callback
	mu.Lock()
	if seen["c"] != CauseExpired {
		t.Fatalf("expiry callback cause = %v", seen["c"])
	}
	mu.Unlock()
}

func TestCloseAndReuse(t *testing.T) {
	c := New[string, int](nil)
	c.Set("a", 1)
	c.Close()
	c.Close() // idempotent

	// after Close the janitor is stopped but data operations still work
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatalf("Get after Close = (%d, %v)", v, ok)
	}
	c.Set("b", 2)
	if v, ok := c.Get("b"); !ok || v != 2 {
		t.Fatalf("Set/Get after Close failed")
	}
}

func TestConcurrentMixed(t *testing.T) {
	c := New[int, int](&Config[int, int]{
		Shards:     8,
		MaxEntries: 5000,
	})
	defer c.Close()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k := (i*id + g) % 3000
				switch i % 5 {
				case 0:
					c.Set(k, i)
				case 1:
					c.Get(k)
				case 2:
					c.Delete(k)
				case 3:
					c.GetOrSet(k, i)
				case 4:
					c.Len()
					c.Stats()
				}
			}
		}(g)
	}
	wg.Wait()
}

func itoa(i int) string {
	return strconv.Itoa(i)
}
