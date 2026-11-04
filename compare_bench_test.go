package gocache

// ---------------------------------------------------------------------------
// 与 cache2go 的同负载对比基准。
//
// 两侧使用相同的键类型（string/int）、相同的 int 值载荷和相同的键空间
// （读类基准预填充 65536 条，并发 Set 的键空间随 b.N 增长）。运行：
//
//	go test -run '^$' -bench 'BenchmarkCompare' -benchmem .
//
// 依赖 go.mod 中的 replace 指令指向同目录的 cache2go 检出副本。
// ---------------------------------------------------------------------------

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muesli/cache2go"
)

const compareKeyspace = 65536

// cache2goTable 以基准名注册一张 cache2go 表；基准结束时清空，避免共享
// 全局注册表中的残留条目污染后续基准。
func cache2goTable(b *testing.B) *cache2go.CacheTable {
	tbl := cache2go.Cache("bench-" + b.Name())
	b.Cleanup(func() { tbl.Flush() })
	return tbl
}

func prefillCache2goInts(tbl *cache2go.CacheTable, n int) {
	for i := 0; i < n; i++ {
		tbl.Add(i, 0, i) // lifeSpan=0：不过期
	}
}

func prefillCache2goStrings(tbl *cache2go.CacheTable, n int) {
	for i := 0; i < n; i++ {
		tbl.Add(strconv.Itoa(i), 0, i)
	}
}

// --- Set（串行，string 键） -------------------------------------------------

func BenchmarkCompareCache2goSetString(b *testing.B) {
	tbl := cache2goTable(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tbl.Add(strconv.Itoa(i), 0, i)
	}
}

func BenchmarkCompareGoCacheSetString(b *testing.B) {
	c := New[string, int](nil)
	defer c.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(strconv.Itoa(i), i)
	}
}

// --- Get 命中（串行，string 键） --------------------------------------------

func BenchmarkCompareCache2goGetHitString(b *testing.B) {
	tbl := cache2goTable(b)
	prefillCache2goStrings(tbl, compareKeyspace)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tbl.Value(strconv.Itoa(i & (compareKeyspace - 1)))
	}
}

func BenchmarkCompareGoCacheGetHitString(b *testing.B) {
	c := New[string, int](nil)
	defer c.Close()
	for i := 0; i < compareKeyspace; i++ {
		c.Set(strconv.Itoa(i), i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(strconv.Itoa(i & (compareKeyspace - 1)))
	}
}

// --- Get 命中（串行，int 键） -----------------------------------------------

func BenchmarkCompareCache2goGetHitInt(b *testing.B) {
	tbl := cache2goTable(b)
	prefillCache2goInts(tbl, compareKeyspace)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tbl.Value(i & (compareKeyspace - 1))
	}
}

func BenchmarkCompareGoCacheGetHitInt(b *testing.B) {
	c := New[int, int](nil)
	defer c.Close()
	for i := 0; i < compareKeyspace; i++ {
		c.Set(i, i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(i & (compareKeyspace - 1))
	}
}

// --- SetSlidingTTL（串行；cache2go 的 Add 即滑动过期语义） ------------------

func BenchmarkCompareCache2goSetSlidingTTL(b *testing.B) {
	tbl := cache2goTable(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tbl.Add(strconv.Itoa(i), time.Minute, i)
	}
}

func BenchmarkCompareGoCacheSetSlidingTTL(b *testing.B) {
	c := New[string, int](nil)
	defer c.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.SetSlidingTTL(strconv.Itoa(i), i, time.Minute)
	}
}

// --- 并发 Get（int 键） -----------------------------------------------------

func BenchmarkCompareCache2goParallelGetInt(b *testing.B) {
	tbl := cache2goTable(b)
	prefillCache2goInts(tbl, compareKeyspace)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			tbl.Value(i & (compareKeyspace - 1))
			i++
		}
	})
}

func BenchmarkCompareGoCacheParallelGetInt(b *testing.B) {
	c := New[int, int](nil)
	defer c.Close()
	for i := 0; i < compareKeyspace; i++ {
		c.Set(i, i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Get(i & (compareKeyspace - 1))
			i++
		}
	})
}

// --- 并发 Set（int 键） -----------------------------------------------------

func BenchmarkCompareCache2goParallelSetInt(b *testing.B) {
	tbl := cache2goTable(b)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			tbl.Add(i, 0, i)
			i++
		}
	})
}

func BenchmarkCompareGoCacheParallelSetInt(b *testing.B) {
	c := New[int, int](nil)
	defer c.Close()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Set(i, i)
			i++
		}
	})
}

// --- 并发 Set（string 键） --------------------------------------------------

func BenchmarkCompareCache2goParallelSetString(b *testing.B) {
	tbl := cache2goTable(b)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			tbl.Add(strconv.Itoa(i), 0, i)
			i++
		}
	})
}

func BenchmarkCompareGoCacheParallelSetString(b *testing.B) {
	c := New[string, int](nil)
	defer c.Close()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Set(strconv.Itoa(i), i)
			i++
		}
	})
}

// --- 并发 NotFoundAdd（insert-if-absent，键空间大部分已存在） --------------

func BenchmarkCompareCache2goNotFoundAdd(b *testing.B) {
	tbl := cache2goTable(b)
	prefillCache2goInts(tbl, compareKeyspace)
	var added, idle atomic.Int32
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := i & (compareKeyspace - 1)
			if tbl.NotFoundAdd(k, 0, i) {
				added.Add(1)
			} else {
				idle.Add(1)
			}
			i++
		}
	})
	_, _ = added.Load(), idle.Load()
}

func BenchmarkCompareGoCacheNotFoundAdd(b *testing.B) {
	c := New[int, int](nil)
	defer c.Close()
	for i := 0; i < compareKeyspace; i++ {
		c.Set(i, i)
	}
	var added, idle atomic.Int32
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := i & (compareKeyspace - 1)
			if c.SetIfAbsent(k, i) {
				added.Add(1)
			} else {
				idle.Add(1)
			}
			i++
		}
	})
	_, _ = added.Load(), idle.Load()
}

// --- 并发混合读写（4 读 1 写，int 键） --------------------------------------

func BenchmarkCompareCache2goParallelMixed(b *testing.B) {
	tbl := cache2goTable(b)
	prefillCache2goInts(tbl, compareKeyspace)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := i & (compareKeyspace - 1)
			if i%5 < 4 {
				tbl.Value(k)
			} else {
				tbl.Add(k, 0, i)
			}
			i++
		}
	})
}

func BenchmarkCompareGoCacheParallelMixed(b *testing.B) {
	c := New[int, int](nil)
	defer c.Close()
	for i := 0; i < compareKeyspace; i++ {
		c.Set(i, i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := i & (compareKeyspace - 1)
			if i%5 < 4 {
				c.Get(k)
			} else {
				c.Set(k, i)
			}
			i++
		}
	})
}
