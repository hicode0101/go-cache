package gocache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGetOrLoadBasic(t *testing.T) {
	var calls atomic.Int32
	c := New[string, string](&Config[string, string]{
		Loader: func(ctx context.Context, key string, args ...any) (string, error) {
			calls.Add(1)
			return "loaded-" + key, nil
		},
	})
	defer c.Close()

	v, err := c.GetOrLoad(context.Background(), "a")
	if err != nil || v != "loaded-a" {
		t.Fatalf("GetOrLoad = (%q, %v)", v, err)
	}
	// second call must come from cache
	v, err = c.GetOrLoad(context.Background(), "a")
	if err != nil || v != "loaded-a" {
		t.Fatalf("second GetOrLoad = (%q, %v)", v, err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("loader called %d times, want 1", n)
	}
}

func TestGetOrLoadArgsForwarded(t *testing.T) {
	c := New[string, string](&Config[string, string]{
		Loader: func(ctx context.Context, key string, args ...any) (string, error) {
			return fmt.Sprintf("%v|%v", key, args), nil
		},
	})
	defer c.Close()

	v, err := c.GetOrLoad(context.Background(), "k", 1, "two")
	if err != nil {
		t.Fatal(err)
	}
	want := "k|[1 two]"
	if v != want {
		t.Fatalf("v = %q, want %q", v, want)
	}
}

func TestGetOrLoadErrorPropagated(t *testing.T) {
	boom := errors.New("boom")
	c := New[string, int](&Config[string, int]{
		Loader: func(ctx context.Context, key string, args ...any) (int, error) {
			return 0, boom
		},
	})
	defer c.Close()

	if _, err := c.GetOrLoad(context.Background(), "a"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the loader's error", err)
	}
	if c.Exists("a") {
		t.Fatal("failed load must not cache anything")
	}
	if st := c.Stats(); st.LoadErrors != 1 || st.Loads != 0 {
		t.Fatalf("load stats = %+v", st)
	}
}

func TestGetOrLoadNoLoader(t *testing.T) {
	c := New[string, int](nil)
	defer c.Close()
	if _, err := c.GetOrLoad(context.Background(), "a"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("err = %v, want ErrKeyNotFound", err)
	}
}

func TestGetOrLoadStampede(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	c := New[int, int](&Config[int, int]{
		Loader: func(ctx context.Context, key int, args ...any) (int, error) {
			calls.Add(1)
			<-release
			return key * 10, nil
		},
	})
	defer c.Close()

	const goroutines = 64
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			v, err := c.GetOrLoad(context.Background(), 7)
			if err == nil && v != 70 {
				err = fmt.Errorf("wrong value %d", v)
			}
			errs[id] = err
		}(g)
	}
	time.Sleep(100 * time.Millisecond) // let them all pile up
	close(release)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("loader called %d times under stampede, want 1", n)
	}
	if st := c.Stats(); st.CoalescedLoads != int64(goroutines-1) {
		t.Fatalf("coalesced = %d, want %d", st.CoalescedLoads, goroutines-1)
	}
}

func TestGetOrLoadParallelDistinctKeys(t *testing.T) {
	c := New[int, int](&Config[int, int]{
		Loader: func(ctx context.Context, key int, args ...any) (int, error) {
			time.Sleep(5 * time.Millisecond)
			return key, nil
		},
	})
	defer c.Close()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				k := id*1000 + i
				v, err := c.GetOrLoad(context.Background(), k)
				if err != nil || v != k {
					t.Errorf("key %d = (%d, %v)", k, v, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if c.Len() != 400 {
		t.Fatalf("Len = %d, want 400", c.Len())
	}
}

func TestGetOrLoadPanicPropagates(t *testing.T) {
	c := New[int, int](&Config[int, int]{
		Loader: func(ctx context.Context, key int, args ...any) (int, error) {
			panic("loader exploded")
		},
	})
	defer c.Close()

	// leader sees the panic
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("panic must propagate to the leader")
			}
		}()
		_, _ = c.GetOrLoad(context.Background(), 1)
	}()

	// a waiter joining an in-flight panicking load sees it too
	c2 := New[int, int](&Config[int, int]{
		Loader: func(ctx context.Context, key int, args ...any) (int, error) {
			time.Sleep(80 * time.Millisecond)
			panic("late boom")
		},
	})
	defer c2.Close()

	leaderDone := make(chan struct{})
	go func() {
		defer func() {
			recover() // leader's panic is expected; swallow it here
			close(leaderDone)
		}()
		_, _ = c2.GetOrLoad(context.Background(), 1)
	}()
	time.Sleep(20 * time.Millisecond) // let the leader take off

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("panic must propagate to the waiter")
			}
		}()
		_, _ = c2.GetOrLoad(context.Background(), 1)
	}()
	<-leaderDone

	// after a panicking load, a fresh call retries the loader normally
	called := 0
	c3 := New[int, int](&Config[int, int]{
		Loader: func(ctx context.Context, key int, args ...any) (int, error) {
			called++
			if called == 1 {
				panic("first attempt fails")
			}
			return 42, nil
		},
	})
	defer c3.Close()

	func() {
		defer func() { recover() }()
		_, _ = c3.GetOrLoad(context.Background(), 1)
	}()
	if v, err := c3.GetOrLoad(context.Background(), 1); err != nil || v != 42 {
		t.Fatalf("retry after panic = (%d, %v), want (42, nil)", v, err)
	}
}

func TestGetOrLoadContextCancel(t *testing.T) {
	release := make(chan struct{})
	c := New[int, int](&Config[int, int]{
		Loader: func(ctx context.Context, key int, args ...any) (int, error) {
			<-release
			return key, nil
		},
	})
	defer c.Close()

	// leader starts the load
	go func() {
		_, _ = c.GetOrLoad(context.Background(), 1)
	}()
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := c.GetOrLoad(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter err = %v, want DeadlineExceeded", err)
	}
	close(release)
}

func TestComputeIfAbsent(t *testing.T) {
	c := New[string, int](nil)
	defer c.Close()

	var calls atomic.Int32
	fn := func(ctx context.Context, key string) (int, error) {
		calls.Add(1)
		return len(key), nil
	}
	v, err := c.ComputeIfAbsent(context.Background(), "abcd", fn)
	if err != nil || v != 4 {
		t.Fatalf("= (%d, %v)", v, err)
	}
	v, err = c.ComputeIfAbsent(context.Background(), "abcd", fn)
	if err != nil || v != 4 {
		t.Fatalf("= (%d, %v)", v, err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("fn called %d times", n)
	}
}

func TestLoadTTL(t *testing.T) {
	c := New[string, int](&Config[string, int]{
		Loader: func(ctx context.Context, key string, args ...any) (int, error) {
			return 1, nil
		},
		LoadTTL: 300 * time.Millisecond,
	})
	defer c.Close()

	if _, err := c.GetOrLoad(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	item, ok := c.GetItem("a")
	if !ok || item.LifeSpan != 300*time.Millisecond {
		t.Fatalf("loaded item lifespan = %v (ok=%v), want 300ms", item.LifeSpan, ok)
	}
	time.Sleep(450 * time.Millisecond)
	if c.Exists("a") {
		t.Fatal("loaded entry should have expired")
	}
}
