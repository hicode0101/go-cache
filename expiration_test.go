package gocache

import (
	"sync"
	"testing"
	"time"
)

func TestAbsoluteTTLExpiry(t *testing.T) {
	c := New[string, int](nil)
	defer c.Close()

	c.SetTTL("a", 1, 200*time.Millisecond)
	c.Set("b", 2) // no TTL

	if _, ok := c.Get("a"); !ok {
		t.Fatal("a should be alive immediately")
	}
	time.Sleep(350 * time.Millisecond)
	if _, ok := c.Get("a"); ok {
		t.Fatal("a should have expired (absolute TTL is not extended by reads)")
	}
	if _, ok := c.Get("b"); !ok {
		t.Fatal("b must never expire")
	}
	if st := c.Stats(); st.Expired == 0 {
		t.Fatal("expiry should be counted")
	}
}

func TestSlidingTTLRefresh(t *testing.T) {
	c := New[string, int](nil)
	defer c.Close()

	// sliding semantics: lifespan renews on every access
	c.SetSlidingTTL("s", 1, 300*time.Millisecond)
	for i := 0; i < 4; i++ {
		time.Sleep(150 * time.Millisecond)
		if _, ok := c.Get("s"); !ok {
			t.Fatalf("s expired too early at iteration %d", i)
		}
	}
	time.Sleep(450 * time.Millisecond)
	if _, ok := c.Get("s"); ok {
		t.Fatal("s should expire after the last access + TTL")
	}
}

func TestLazyExpiryOnRead(t *testing.T) {
	// disable the janitor's precision by giving it a huge cap: expiry
	// must still be observed on read (lazy removal), and Len must not
	// pretend the entry is gone before a sweep.
	c := New[string, int](&Config[string, int]{
		CleanupInterval: 10 * time.Minute,
	})
	defer c.Close()

	c.SetTTL("a", 1, 200*time.Millisecond)
	time.Sleep(400 * time.Millisecond)
	if _, ok := c.Get("a"); ok {
		t.Fatal("expired entry must be treated as absent on Get")
	}
	if c.Exists("a") {
		t.Fatal("Exists must treat expired entry as absent")
	}
}

func TestJanitorSweeps(t *testing.T) {
	c := New[int, int](&Config[int, int]{
		CleanupInterval: 50 * time.Millisecond,
	})
	defer c.Close()

	const n = 500
	for i := 0; i < n; i++ {
		c.SetTTL(i, i, 100*time.Millisecond)
	}
	time.Sleep(400 * time.Millisecond)
	if got := c.Len(); got != 0 {
		t.Fatalf("Len after sweep = %d, want 0", got)
	}
	if st := c.Stats(); st.Expired < n {
		t.Fatalf("expired counter = %d, want >= %d", st.Expired, n)
	}
}

func TestJanitorSweepsAcrossShards(t *testing.T) {
	c := New[int, int](&Config[int, int]{
		Shards:          32,
		CleanupInterval: 40 * time.Millisecond,
	})
	defer c.Close()

	for i := 0; i < 2000; i++ {
		c.SetTTL(i, i, 90*time.Millisecond)
	}
	time.Sleep(350 * time.Millisecond)
	if got := c.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0", got)
	}
}

func TestReplaceReschedulesExpiry(t *testing.T) {
	c := New[string, int](nil)
	defer c.Close()

	c.SetTTL("a", 1, 200*time.Millisecond)
	time.Sleep(80 * time.Millisecond)
	// overwrite with a longer TTL: the old heap record must not fire
	c.SetTTL("a", 2, 600*time.Millisecond)
	time.Sleep(300 * time.Millisecond)
	if _, ok := c.Get("a"); !ok {
		t.Fatal("replaced entry must honor its NEW ttl, not the old one")
	}
	time.Sleep(600 * time.Millisecond)
	if _, ok := c.Get("a"); ok {
		t.Fatal("replaced entry must eventually expire by its new ttl")
	}
}

func TestReplaceWithNoTTLNeverExpires(t *testing.T) {
	c := New[string, int](nil)
	defer c.Close()

	c.SetTTL("a", 1, 200*time.Millisecond)
	time.Sleep(80 * time.Millisecond)
	c.Set("a", 2) // clear TTL
	time.Sleep(350 * time.Millisecond)
	if _, ok := c.Get("a"); !ok {
		t.Fatal("overwrite without TTL must clear expiration")
	}
	if item, ok := c.GetItem("a"); ok && item.ExpireAt.Unix() != 0 {
		t.Fatalf("ExpireAt should be zero, got %v", item.ExpireAt)
	}
}

func TestExpiryCallbackCause(t *testing.T) {
	var mu sync.Mutex
	causes := map[string]RemoveCause{}
	c := New[string, int](&Config[string, int]{
		OnRemoved: func(k string, v int, cause RemoveCause) {
			mu.Lock()
			causes[k] = cause
			mu.Unlock()
		},
	})
	defer c.Close()

	c.SetTTL("a", 1, 200*time.Millisecond)
	time.Sleep(350 * time.Millisecond)
	c.Get("a") // lazy removal fires the callback

	mu.Lock()
	defer mu.Unlock()
	if causes["a"] != CauseExpired {
		t.Fatalf("cause = %v, want expired", causes["a"])
	}
}

func TestExpiryJitter(t *testing.T) {
	const jitter = 300 * time.Millisecond
	c := New[int, int](&Config[int, int]{
		ExpiryJitter:    jitter,
		CleanupInterval: time.Hour,
	})
	defer c.Close()

	base := time.Now()
	c.SetTTL(0, 0, 50*time.Millisecond)
	item, ok := c.GetItem(0)
	if !ok {
		t.Fatal("item missing")
	}
	life := item.ExpireAt.Sub(base)
	if life < 50*time.Millisecond || life > 50*time.Millisecond+jitter+50*time.Millisecond {
		t.Fatalf("lifetime %v outside [ttl, ttl+jitter+slack]", life)
	}
	_ = c
}

func TestSetExpiryKeepsStatsConsistent(t *testing.T) {
	c := New[int, int](nil)
	defer c.Close()

	for i := 0; i < 100; i++ {
		c.SetTTL(i, i, 50*time.Millisecond)
		c.Set(i, i) // overwrite the same keys
	}
	if c.Len() != 100 {
		t.Fatalf("Len = %d, want 100", c.Len())
	}
	st := c.Stats()
	if st.Sets != 100 || st.Updates != 100 {
		t.Fatalf("sets = %d, updates = %d", st.Sets, st.Updates)
	}
	if st.Cost != 100 {
		t.Fatalf("cost = %d, want 100", st.Cost)
	}
}

func TestDeleteReinsertKeepsNewDeadline(t *testing.T) {
	// The stale heap record of the deleted entry must not remove the
	// freshly re-inserted entry when its (earlier) deadline fires.
	c := New[string, int](&Config[string, int]{
		CleanupInterval: 20 * time.Millisecond,
	})
	defer c.Close()

	c.SetTTL("k", 1, 60*time.Millisecond)
	if !c.Delete("k") {
		t.Fatal("delete failed")
	}
	c.SetTTL("k", 2, 700*time.Millisecond)

	time.Sleep(200 * time.Millisecond) // old deadline long past
	if v, ok := c.Get("k"); !ok || v != 2 {
		t.Fatalf("re-inserted entry killed by stale expiry record: (%v, %v)", v, ok)
	}
	time.Sleep(700 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Fatal("re-inserted entry must expire by its own ttl")
	}
}

func TestFlushThenReinsertStillSwept(t *testing.T) {
	// After a Flush the janitor must still wake for newly inserted
	// expiring entries (Flush resets the per-shard deadline bookkeeping).
	c := New[int, int](&Config[int, int]{
		CleanupInterval: 20 * time.Millisecond,
	})
	defer c.Close()

	for i := 0; i < 100; i++ {
		c.SetTTL(i, i, time.Hour)
	}
	c.Flush()

	// no reads from here on: only the janitor may remove these
	for i := 0; i < 100; i++ {
		c.SetTTL(i, i, 80*time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	if got := c.Len(); got != 0 {
		t.Fatalf("Len after re-inserted entries expired = %d, want 0", got)
	}
}

func TestConcurrentFlushWithTTLWrites(t *testing.T) {
	c := New[int, int](&Config[int, int]{
		CleanupInterval: 10 * time.Millisecond,
	})
	defer c.Close()

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := (i + id) % 200
				c.SetTTL(k, i, 50*time.Millisecond)
				c.Get(k)
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			c.Flush()
			time.Sleep(2 * time.Millisecond)
		}
	}()
	wg.Wait()
	// final state must be internally consistent: every surviving entry
	// is live and the janitor eventually drains it
	deadline := time.Now().Add(2 * time.Second)
	for c.Len() != 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := c.Len(); got != 0 {
		t.Fatalf("Len after settle = %d, want 0", got)
	}
}

func TestConcurrentExpiry(t *testing.T) {
	c := New[int, int](&Config[int, int]{
		CleanupInterval: 30 * time.Millisecond,
		Shards:          8,
	})
	defer c.Close()

	var wg sync.WaitGroup
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := (i + id*7) % 400
				c.SetTTL(k, i, 40*time.Millisecond)
				c.Get(k)
			}
		}(g)
	}
	wg.Wait()
	// the janitor sweeps asynchronously; poll instead of assuming a
	// fixed latency (CI machines can be slow)
	deadline := time.Now().Add(2 * time.Second)
	for c.Len() != 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := c.Len(); got != 0 {
		t.Fatalf("Len after everything expired = %d, want 0", got)
	}
}
