# go-cache

高性能、泛型、分片的 Go 进程内缓存库

性能报告见 [PERFORMANCE.md](PERFORMANCE.md)。

## 特性

**新增能力**

- **泛型**：`Cache[K comparable, V any]`，编译期类型安全，零接口装箱
- **锁分片**：默认 16 分片（可配，自动取 2 的幂），并发读写线性扩展
- **双 TTL 语义**：绝对 TTL（`WithTTL`）+ 滑动 TTL（`WithSlidingTTL`）+ 过期抖动（`ExpiryJitter`，防集体失效）
- **容量与内存上限**：`MaxEntries` / `MaxMemory` + `CostFunc`；淘汰策略 LRU（默认）/ LFU（Redis 风格 8 位对数计数 + 惰性衰减 + 采样）/ None
- **Singleflight**：`GetOrLoad` / `ComputeIfAbsent` 合并并发加载，防缓存击穿；panic 全员透传、等待方可被 context 取消
- **原子操作**：`GetOrSet`（sync.Map 语义）、`GetAndSet`、`CompareAndSwap`（谓词式）、`CompareAndDelete`
- **批量与迭代**：`GetMany` / `SetMany` / `DeleteMany`、`Keys()`、`ForEach`（回调锁外执行）、`All()` / `AllItems()`（range-over-func 迭代器，Go 1.23+）
- **统计**：Hits/Misses/命中率/Sets/Updates/Expired/Evicted/Loads/CoalescedLoads，`Stats()` / `ResetStats()`
- **生命周期**：`Close()` 幂等；忘记 Close 有 finalizer 兜底回收 janitor
- **过期机制**：每分片过期最小堆（只清到期条目）+ 读路径 O(1) 惰性过期 + 精确自适应唤醒 janitor

## 安装

```bash
go get github.com/hicode0101/go-cache
```

要求 Go 1.24+。

## 快速上手

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/hicode0101/go-cache"
)

func main() {
	// 最简用法：nil 配置 = 全默认
	cache := gocache.New[string, int](nil)
	defer cache.Close()

	cache.Set("a", 1)                          // 永不过期
	cache.SetTTL("b", 2, time.Minute)          // 绝对 TTL
	cache.SetSlidingTTL("c", 3, 5*time.Second) // 滑动 TTL

	v, ok := cache.Get("a") // Get 会续滑动 TTL、更新访问计数与 LRU 顺序
	_ = v
	_ = ok

	// 完整配置
	loaded := gocache.New[string, *User](&gocache.Config[string, *User]{
		Shards:       32,
		MaxEntries:   100_000,
		Policy:       gocache.PolicyLRU,
		LoadTTL:      10 * time.Minute,
		ExpiryJitter: time.Second,
		OnRemoved:    func(k string, u *User, cause gocache.RemoveCause) { /* 释放资源 */ },
		Loader: func(ctx context.Context, key string, args ...any) (*User, error) {
			return loadUserFromDB(ctx, key) // 并发 miss 只会触发一次（singleflight）
		},
	})
	defer loaded.Close()

	u, err := loaded.GetOrLoad(context.Background(), "user:1")
	_ = u
	_ = err
}

func loadUserFromDB(ctx context.Context, key string) (*User, error) { return nil, nil }

type User struct{ Name string }
```

## 配置参考

| 字段 | 说明 | 默认 |
|---|---|---|
| `Shards` | 锁分片数（自动向上取 2 的幂） | 16 |
| `Policy` | 淘汰策略：`PolicyLRU` / `PolicyLFU` / `PolicyNone` | LRU |
| `MaxEntries` | 最大条目数（0 = 不限） | 0 |
| `MaxMemory` | 内存预算字节（配合 `CostFunc`；0 = 不限） | 0 |
| `CostFunc` | 条目内存成本；nil 时每条计 1 | nil |
| `HashFunc` | 分片哈希；nil 用运行时哈希 | nil |
| `Loader` / `LoadTTL` | 默认加载器及其 TTL | nil / 0（不过期） |
| `OnAdded` / `OnReplaced` / `OnRemoved` | 生命周期回调（全部锁外执行，可安全回调本缓存） | nil |
| `CleanupInterval` | janitor 兜底睡眠上限 | 1 分钟 |
| `ExpiryJitter` | 过期时间随机抖动上限 | 0 |
| `Logger` | 日志（`gocache.NopLogger` / `NewStdLogger` / 自接） | 静默 |

`Set` 级选项：`WithTTL(d)`、`WithSlidingTTL(d)`、`WithCost(n)`。

## 命名表

```go
c1, err := gocache.GetOrCreate[string, User]("sessions", nil) // 首次创建
c2, err := gocache.GetOrCreate[string, User]("sessions", nil) // 同一实例
_, err = gocache.GetOrCreate[int, int]("sessions", nil)       // ErrTypeMismatch

gocache.TableNames()            // 已注册表名
gocache.DeleteTable("sessions") // 移除并 Close
```

## 测试

```bash
go test -count=1 .                       # 单元测试
CGO_ENABLED=1 go test -race -count=3 .   # 竞态检测（需 C 编译器）
go test -run '^$' -bench . -benchmem .   # 基准（见 PERFORMANCE.md）
```

## 语义差异

- 覆盖写触发 `OnReplaced` 而非 `OnAdded`；
- `Flush` 会触发移除回调（cause=flushed）；
- 加载器返回 `error` 时错误原样透传给调用方；
- 回调全部在锁外执行，杜绝持锁回调导致的重入死锁风险；
- 过期条目在 `SetIfAbsent`/`GetOrSet`/覆盖写时被视为"不存在"。

## License

MIT
