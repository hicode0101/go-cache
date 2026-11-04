package gocache

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Core operations (serial)
// ---------------------------------------------------------------------------

func BenchmarkSet(b *testing.B) {
	c := New[int, int](nil)
	defer c.Close()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.Set(i, i)
	}
}

func BenchmarkSetWithTTL(b *testing.B) {
	c := New[int, int](nil)
	defer c.Close()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.SetTTL(i, i, time.Minute)
	}
}

func BenchmarkGetHit(b *testing.B) {
	c := New[int, int](nil)
	defer c.Close()
	for i := 0; i < 65536; i++ {
		c.Set(i, i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get(i & 65535)
	}
}

func BenchmarkGetMiss(b *testing.B) {
	c := New[int, int](nil)
	defer c.Close()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get(-i - 1)
	}
}

func BenchmarkDelete(b *testing.B) {
	c := New[int, int](nil)
	defer c.Close()
	for i := 0; i < b.N; i++ {
		c.Set(i, i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.Delete(i)
	}
}

// ---------------------------------------------------------------------------
// Parallel throughput — the headline numbers.
// ---------------------------------------------------------------------------

func benchmarkParallelGet(b *testing.B, shards int) {
	c := New[int, int](&Config[int, int]{Shards: shards})
	defer c.Close()
	for i := 0; i < 65536; i++ {
		c.Set(i, i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = c.Get(i & 65535)
			i++
		}
	})
}

func BenchmarkParallelGet(b *testing.B) {
	for _, shards := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			benchmarkParallelGet(b, shards)
		})
	}
}

func benchmarkParallelSet(b *testing.B, shards int) {
	c := New[int, int](&Config[int, int]{Shards: shards})
	defer c.Close()
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Set(i, i)
			i++
		}
	})
}

func BenchmarkParallelSet(b *testing.B) {
	for _, shards := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			benchmarkParallelSet(b, shards)
		})
	}
}

// BenchmarkParallelSetGet mixes reads and writes 4:1, the typical cache
// workload shape.
func BenchmarkParallelSetGet(b *testing.B) {
	c := New[int, int](nil)
	defer c.Close()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%5 == 0 {
				c.Set(i&65535, i)
			} else {
				_, _ = c.Get(i & 65535)
			}
			i++
		}
	})
}

// BenchmarkNotFoundAdd measures the insert-if-absent fast path.
func BenchmarkNotFoundAdd(b *testing.B) {
	c := New[int, int](nil)
	defer c.Close()
	var added, idle atomic.Int32
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if c.SetIfAbsent(i, i) {
				added.Add(1)
			} else {
				idle.Add(1)
			}
			i++
		}
	})
}

// ---------------------------------------------------------------------------
// Loading / singleflight
// ---------------------------------------------------------------------------

func BenchmarkGetOrLoadHit(b *testing.B) {
	c := New[int, int](&Config[int, int]{
		Loader: func(ctx context.Context, key int, args ...any) (int, error) {
			return key, nil
		},
	})
	defer c.Close()
	for i := 0; i < 65536; i++ {
		_, _ = c.GetOrLoad(context.Background(), i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = c.GetOrLoad(context.Background(), i&65535)
	}
}

func BenchmarkGetOrLoadMiss(b *testing.B) {
	c := New[int, int](&Config[int, int]{
		Loader: func(ctx context.Context, key int, args ...any) (int, error) {
			return key, nil
		},
	})
	defer c.Close()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = c.GetOrLoad(context.Background(), i)
	}
}

func BenchmarkParallelGetOrLoad(b *testing.B) {
	c := New[int, int](&Config[int, int]{
		Loader: func(ctx context.Context, key int, args ...any) (int, error) {
			return key, nil
		},
	})
	defer c.Close()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = c.GetOrLoad(context.Background(), i&65535)
			i++
		}
	})
}

// ---------------------------------------------------------------------------
// Expiration machinery
// ---------------------------------------------------------------------------

func BenchmarkSetExpiring1M(b *testing.B) {
	// 1M live TTL entries: measures heap push + shard map insert at scale
	c := New[int, int](&Config[int, int]{
		Shards:          256,
		CleanupInterval: time.Hour,
	})
	defer c.Close()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.SetTTL(i, i, time.Hour)
	}
}

func BenchmarkSweep1MExpired(b *testing.B) {
	// measures the janitor's cost of draining 1M due entries
	c := New[int, int](&Config[int, int]{
		Shards:          256,
		CleanupInterval: time.Hour,
	})
	defer c.Close()
	for i := 0; i < 1_000_000; i++ {
		c.SetTTL(i, i, 50*time.Millisecond)
	}
	// stop the janitor so it cannot steal the measurement
	c.Close()
	time.Sleep(80 * time.Millisecond) // make everything due

	// The work is a one-shot pass, so measure it directly and report
	// absolute metrics; ns/op of the harness loop itself is meaningless
	// here.
	b.ResetTimer()
	b.StopTimer()

	start := time.Now()
	for _, s := range c.shards {
		var fires []func()
		s.mu.Lock()
		c.sweepShardLocked(s, &fires)
		s.mu.Unlock()
		runFires(fires)
	}
	elapsed := time.Since(start)

	b.ReportMetric(float64(elapsed.Nanoseconds())/1e6, "ms-total")
	b.ReportMetric(float64(elapsed.Nanoseconds())/1_000_000, "ns/item")
	if c.Len() != 0 {
		b.Fatalf("sweep left %d entries", c.Len())
	}
}

// ---------------------------------------------------------------------------
// Introspection
// ---------------------------------------------------------------------------

func BenchmarkMostAccessed100(b *testing.B) {
	c := New[int, int](nil)
	defer c.Close()
	for i := 0; i < 10_000; i++ {
		c.Set(i, i)
		for j := 0; j < i%7; j++ {
			_, _ = c.Get(i)
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = c.MostAccessed(100)
	}
}

func BenchmarkForEach10K(b *testing.B) {
	c := New[int, int](nil)
	defer c.Close()
	for i := 0; i < 10_000; i++ {
		c.Set(i, i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.ForEach(func(k, v int) bool { return true })
	}
}

func BenchmarkBatchGet100(b *testing.B) {
	c := New[int, int](nil)
	defer c.Close()
	keys := make([]int, 100)
	for i := 0; i < 10_000; i++ {
		c.Set(i, i)
		if i < 100 {
			keys[i] = i
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = c.GetMany(keys...)
	}
}

func BenchmarkStringKeys(b *testing.B) {
	c := New[string, int](nil)
	defer c.Close()
	keys := make([]string, 65536)
	for i := range keys {
		keys[i] = "user:session:" + strconv.Itoa(i)
		c.Set(keys[i], i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get(keys[i&65535])
	}
}
