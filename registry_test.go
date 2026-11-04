package gocache

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRegistrySameInstance(t *testing.T) {
	a, err := GetOrCreate[string, int]("reg-test-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer DeleteTable("reg-test-1")
	b, err := GetOrCreate[string, int]("reg-test-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("GetOrCreate must return the same instance for the same name")
	}
	a.Set("k", 42)
	if v, ok := b.Get("k"); !ok || v != 42 {
		t.Fatalf("instances differ: (%d, %v)", v, ok)
	}
}

func TestRegistryTypeMismatch(t *testing.T) {
	_, err := GetOrCreate[string, int]("reg-test-2", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer DeleteTable("reg-test-2")
	if _, err := GetOrCreate[string, string]("reg-test-2", nil); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("err = %v, want ErrTypeMismatch", err)
	}
	if _, err := GetOrCreate[int, int]("reg-test-2", nil); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("err = %v, want ErrTypeMismatch", err)
	}
	// same types still fine
	if _, err := GetOrCreate[string, int]("reg-test-2", nil); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryLookupAndDelete(t *testing.T) {
	DeleteTable("reg-test-3") // clean slate from earlier runs
	if _, err := Table[string, int]("reg-test-3"); !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("err = %v, want ErrTableNotFound", err)
	}
	orig, err := GetOrCreate[int, string]("reg-test-3", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer DeleteTable("reg-test-3")
	if got, err := Table[int, string]("reg-test-3"); err != nil || got != orig {
		t.Fatalf("Table = (%v, %v)", got, err)
	}
	if !DeleteTable("reg-test-3") {
		t.Fatal("DeleteTable should report true")
	}
	if DeleteTable("reg-test-3") {
		t.Fatal("second DeleteTable should report false")
	}
	if _, err := Table[int, string]("reg-test-3"); !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("err = %v, want ErrTableNotFound", err)
	}
	// name is reusable
	if _, err := GetOrCreate[int, string]("reg-test-3", nil); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryNames(t *testing.T) {
	_, _ = GetOrCreate[int, int]("reg-names-a", nil)
	_, _ = GetOrCreate[int, int]("reg-names-b", nil)
	names := TableNames()
	found := 0
	for _, n := range names {
		if n == "reg-names-a" || n == "reg-names-b" {
			found++
		}
	}
	if found != 2 {
		t.Fatalf("found %d of the registered names", found)
	}
	if TableCount() < 2 {
		t.Fatal("TableCount too low")
	}
	// cleanup so other runs are unaffected
	DeleteTable("reg-names-a")
	DeleteTable("reg-names-b")
}

func TestStatsCounters(t *testing.T) {
	c := New[string, int](nil)
	defer c.Close()

	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("a", 3) // update
	c.Get("a")    // hit
	c.Get("a")    // hit
	c.Get("zz")   // miss
	c.Delete("b")

	st := c.Stats()
	if st.Sets != 2 {
		t.Fatalf("Sets = %d, want 2", st.Sets)
	}
	if st.Updates != 1 {
		t.Fatalf("Updates = %d, want 1", st.Updates)
	}
	if st.Hits != 2 || st.Misses != 1 {
		t.Fatalf("Hits = %d, Misses = %d", st.Hits, st.Misses)
	}
	if st.Deletes != 1 {
		t.Fatalf("Deletes = %d", st.Deletes)
	}
	if st.Entries != 1 {
		t.Fatalf("Entries = %d", st.Entries)
	}
	if r := st.HitRate(); r < 0.66 || r > 0.67 {
		t.Fatalf("HitRate = %f, want ~0.667", r)
	}
}

func TestStatsHitRateZeroReads(t *testing.T) {
	c := New[string, int](nil)
	defer c.Close()
	if r := c.Stats().HitRate(); r != 0 {
		t.Fatalf("HitRate = %f, want 0", r)
	}
}

func TestResetStats(t *testing.T) {
	c := New[string, int](nil)
	defer c.Close()

	c.Set("a", 1)
	c.Get("a")
	c.Get("x")
	c.ResetStats()
	st := c.Stats()
	if st.Hits != 0 || st.Misses != 0 || st.Sets != 0 || st.Entries != 1 {
		t.Fatalf("stats after reset = %+v", st)
	}
}

func TestLoggerReceivesLoadError(t *testing.T) {
	var buf bytes.Buffer
	l := log.New(&buf, "", 0)
	c := New[string, int](&Config[string, int]{
		Logger: NewStdLogger(l),
		Loader: func(ctx context.Context, key string, args ...any) (int, error) {
			return 0, errors.New("db down")
		},
	})
	defer c.Close()

	_, _ = c.GetOrLoad(context.Background(), "a")
	if !strings.Contains(buf.String(), "ERROR") || !strings.Contains(buf.String(), "db down") {
		t.Fatalf("log = %q", buf.String())
	}
}

func TestEndToEndUsageFlow(t *testing.T) {
	// end-to-end check of the classic named-table flow
	c := New[string, string](nil)
	defer c.Close()

	c.SetSlidingTTL("someKey", "This is a test!", 50*time.Millisecond)
	if v, ok := c.Get("someKey"); !ok || v != "This is a test!" {
		t.Fatalf("found %q, %v", v, ok)
	}

	var deleted string
	c.SetRemoveCallback("someKey", func(k string, v string, cause RemoveCause) {
		deleted = k
	})
	c.Delete("someKey")
	if deleted != "someKey" {
		t.Fatal("per-entry callback not invoked on Delete")
	}

	c.Set("someKey", "again")
	c.Flush()
	if c.Len() != 0 {
		t.Fatal("flush should empty the table")
	}
}

func TestParallelTableAccess(t *testing.T) {
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := GetOrCreate[string, int]("reg-parallel", nil)
			if err != nil {
				t.Errorf("goroutine %d: %v", id, err)
				return
			}
			for i := 0; i < 100; i++ {
				c.Set("k", id)
				c.Get("k")
			}
		}(g)
	}
	wg.Wait()
	DeleteTable("reg-parallel")
}
