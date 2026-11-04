<div align="center">

# go-cache

**高性能、泛型、分片的 Go 进程内缓存库**

![Go Version](https://img.shields.io/badge/Go-1.24%2B-00ADD8?style=flat-square&logo=go&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-blue?style=flat-square)
![Tests](https://img.shields.io/badge/tests-passing-brightgreen?style=flat-square)
![Race Detector](https://img.shields.io/badge/race%20detector-clean-success?style=flat-square)
![Dependencies](https://img.shields.io/badge/dependencies-zero-success?style=flat-square)

[特性](#-特性) · [快速上手](#-快速上手) · [典型场景](#-典型场景) · [API 一览](#-api-一览) · [配置参考](#-配置参考) · [性能](#-性能)

</div>

---

## 🚀 特性

- **泛型 API** —— `Cache[K comparable, V any]`，编译期类型安全，零接口装箱
- **锁分片** —— 默认 16 分片（可配，自动取 2 的幂），并发读写线性扩展，读路径零分配
- **双 TTL 语义** —— 绝对 TTL（`WithTTL`）+ 滑动 TTL（`WithSlidingTTL`）+ 过期抖动（`ExpiryJitter`，防集体失效）
- **精确过期** —— 每分片过期最小堆（只清到期条目）+ 读路径 O(1) 惰性过期 + 自适应精确唤醒 janitor
- **容量与内存上限** —— `MaxEntries` / `MaxMemory` + `CostFunc`；淘汰策略 LRU（默认）/ LFU / None
- **Singleflight 防击穿** —— `GetOrLoad` / `ComputeIfAbsent` 合并并发加载；panic 全员透传、等待方可被 context 取消
- **原子操作** —— `GetOrSet`（sync.Map 语义）、`GetAndSet`、`CompareAndSwap`、`CompareAndDelete`
- **批量与迭代** —— `GetMany` / `SetMany` / `DeleteMany`、`ForEach`（回调锁外执行）、`All()` / `AllItems()`（range-over-func 迭代器）
- **统计可观测** —— 命中率、Sets/Updates、Expired/Evicted、Loads/CoalescedLoads 等，`Stats()` / `ResetStats()`
- **生命周期管理** —— `Close()` 幂等；忘记 Close 有 finalizer 兜底回收 janitor
- **命名表** —— `GetOrCreate` / `Table`，全局注册表，类型不匹配返回错误而非 panic

## 📦 安装

```bash
go get github.com/hicode0101/go-cache
```

要求 **Go 1.24+**，零第三方依赖。

```go
import "github.com/hicode0101/go-cache" // 包名 gocache
```

## ⚡ 快速上手

```go
package main

import (
	"context"
	"time"

	"github.com/hicode0101/go-cache" // 包名 gocache
)

func main() {
	// 最简用法：nil 配置 = 全默认（16 分片、LRU、无容量上限）
	c := gocache.New[string, int](nil)
	defer c.Close()

	c.Set("a", 1)                          // 永不过期
	c.SetTTL("b", 2, time.Minute)          // 绝对 TTL
	c.SetSlidingTTL("c", 3, 5*time.Second) // 滑动 TTL

	v, ok := c.Get("a") // 命中：续滑动 TTL、访问计数 +1、LRU 置顶
	_ = v
	_ = ok
}
```

## 📖 典型场景

### 1️⃣ 初始化缓存

```go
// 方式一：全默认。16 分片、LRU 淘汰、无上限、静默日志
c := gocache.New[string, int](nil)
defer c.Close() // 幂等；进程常驻缓存可不调用（有 finalizer 兜底）

// 方式二：完整配置（生产环境推荐）
c = gocache.New[string, *User](&gocache.Config[string, *User]{
	Shards:         32,                    // 锁分片数（自动向上取 2 的幂）
	MaxEntries:     100_000,               // 最大条目数
	Policy:         gocache.PolicyLRU,     // 淘汰策略：LRU / LFU / None
	LoadTTL:        10 * time.Minute,      // 加载器写入的条目 TTL
	ExpiryJitter:   time.Second,           // 过期时间随机抖动上限
	CleanupInterval: time.Minute,          // janitor 兜底睡眠上限
	OnRemoved: func(k string, u *User, cause gocache.RemoveCause) {
		// 条目被删除/过期/淘汰/清空时回调（锁外执行，可安全回调本缓存）
	},
	Loader: func(ctx context.Context, key string, args ...any) (*User, error) {
		return loadUserFromDB(ctx, key) // GetOrLoad 的后端加载器
	},
})
```

### 2️⃣ 增加缓存

```go
c.Set("a", 1)                              // 插入，永不过期
c.Set("b", 2, gocache.WithTTL(time.Hour))  // 插入并附带 TTL
c.SetMany(map[string]int{"c": 3, "d": 4}, gocache.WithTTL(time.Minute)) // 批量插入

// 不存在才插入（insert-if-absent），返回是否真的插入了
added := c.SetIfAbsent("lock:order:42", 1)

// 已存在则返回现值（loaded=true），不存在则插入（loaded=false）——sync.Map 语义
actual, loaded := c.GetOrSet("config:timeout", 30)
```

### 3️⃣ 更新缓存

```go
c.Set("a", 100) // 直接覆盖：触发 OnReplaced（不触发 OnAdded），访问计数清零

// 覆盖并取回旧值
old, existed := c.GetAndSet("a", 200)

// 谓词式 CAS：当前值满足 match 才替换，全程原子
swapped := c.CompareAndSwap("a",
	func(old int) bool { return old == 200 }, // 期望的旧值条件
	300,                                      // 新值
	gocache.WithTTL(time.Minute),             // 可选：同时调整 TTL
)
```

### 4️⃣ 读取缓存

```go
v, ok := c.Get("a")        // 命中：续滑动 TTL、访问计数 +1、LRU 置顶
p, ok := c.Peek("a")       // 只读快照：不续期、不计数、不动 LRU（旁路检查用）
item, ok := c.GetItem("a") // 需要元数据时：访问次数/创建时间/剩余 TTL 等
_ = item.AccessCount
_ = item.ExpireAt

m := c.GetMany("a", "b", "missing") // 批量读取，返回 map（缺失的键不在结果中）

d, ok := c.ExpireIn("b") // 剩余存活时间；永不过期或不存在时 ok=false
c.Exists("a")            // 是否存在（不刷新、不计数）
c.KeepAlive("a")         // 只续期不取值（滑动 TTL 场景的心跳）
```

### 5️⃣ 删除缓存

```go
removed := c.Delete("a")             // 是否真的删了
old, ok := c.GetAndDelete("b")       // 删除并取回旧值（取走即删）
val, deleted := c.CompareAndDelete("c", // 值满足条件才删，全程原子
	func(old int) bool { return old == 3 })

n := c.DeleteMany("x", "y", "z") // 批量删除，返回实际删除数
```

### 6️⃣ 设置缓存过期时间

```go
// 绝对 TTL：写入后 10 分钟必过期，读不延期
c.SetTTL("session:token", tok, 10*time.Minute)

// 滑动 TTL：每次访问（Get/GetItem/KeepAlive）自动续期——会话/热点数据
c.SetSlidingTTL("hot:key", 1, 5*time.Minute)

// Set 选项形式（可与其他选项组合）
c.Set("k", v, gocache.WithTTL(time.Hour), gocache.WithCost(1024))

// 手动续期（等价于一次不取值的 Get）
c.KeepAlive("hot:key")

// 查询剩余存活时间
if d, ok := c.ExpireIn("session:token"); ok {
	fmt.Println("还剩", d)
}
```

> 💡 批量写入 + 相同 TTL 的场景（如启动预热）建议配置 `ExpiryJitter`，让过期时间随机错开，避免同一时刻集中失效造成后端加载毛刺。

### 7️⃣ 清空缓存

```go
c.Flush() // 清空全部条目；移除回调以 cause = gocache.CauseFlushed 触发
```

```go
// 命名表的清空 = 移除整张表
gocache.DeleteTable("sessions") // 移除并 Close，表名可复用
```

### 8️⃣ 防击穿加载（Singleflight）

```go
c := gocache.New[string, *User](&gocache.Config[string, *User]{
	Loader:  loadUserFromDB,      // 未命中时的后端加载器
	LoadTTL: 10 * time.Minute,    // 加载结果的 TTL
})
defer c.Close()

// 未命中时加载；同一 key 的并发 miss 只会触发一次加载，结果共享
u, err := c.GetOrLoad(ctx, "user:1", args...)

// 一次性计算也可以享受同样的击穿保护
v, err := c.ComputeIfAbsent(ctx, "report:2026-11", buildReport)
```

### 9️⃣ 遍历与热点分析

```go
// 回调式遍历（回调在锁外执行，可安全回调本缓存），返回 false 提前终止
c.ForEach(func(k string, v int) bool {
	fmt.Println(k, v)
	return true
})

// range-over-func 迭代器（Go 1.23+）
for k, v := range c.All() {
	fmt.Println(k, v)
}
for k, item := range c.AllItems() {
	fmt.Println(k, item.AccessCount)
}

// 访问次数最多的前 10 个条目（n < 0 返回全部）
top := c.MostAccessed(10)
```

### 🔟 容量淘汰与统计

```go
c := gocache.New[string, []byte](&gocache.Config[string, []byte]{
	MaxMemory: 256 << 20, // 256MB 内存预算
	CostFunc: func(k string, v []byte) int64 { return int64(len(v)) },
	Policy:   gocache.PolicyLFU, // 抗一次性扫库污染
})
defer c.Close()

// 运行时观测
st := c.Stats()
fmt.Printf("条目 %d | 命中率 %.1f%% | 过期 %d | 淘汰 %d | 合并加载 %d\n",
	st.Entries, st.HitRate()*100, st.Expired, st.Evicted, st.CoalescedLoads)
c.ResetStats() // 归零，不影响数据
```

### 多表实例（命名表）

每张命名表都是一个**完全独立的缓存实例**（独立分片、独立淘汰策略、独立内存上限、独立回调与 janitor），通过全局注册表按名字共享——适合一个进程内多条业务数据各自配额、互不干扰。

```go
// ── 启动时集中创建多张表（首次 GetOrCreate 决定该表的类型与配置）──
_, err := gocache.GetOrCreate[string, *Session]("sessions", &gocache.Config[string, *Session]{
	MaxEntries: 100_000, // 会话表的独立容量上限
})

_, err = gocache.GetOrCreate[int64, *User]("users", &gocache.Config[int64, *User]{
	Loader:  loadUserFromDB,       // 未命中自动走 DB 加载
	LoadTTL: 10 * time.Minute,
})

_, err = gocache.GetOrCreate[string, []byte]("products", &gocache.Config[string, []byte]{
	Policy:    gocache.PolicyLFU,  // 独立淘汰策略
	MaxMemory: 256 << 20,          // 独立内存预算
	CostFunc:  func(k string, v []byte) int64 { return int64(len(v)) },
})

// ── 任意位置按名取用（拿到的是同一实例，cfg 不会再次生效）──
sessions, _ := gocache.Table[string, *Session]("sessions")
sessions.SetSlidingTTL(token, sess, 30*time.Minute)

users, _ := gocache.Table[int64, *User]("users")
u, err := users.GetOrLoad(ctx, 42)

// 键/值类型必须与创建时完全一致，否则报错而不是 panic：
_, err = gocache.GetOrCreate[int, int]("users", nil) // ErrTypeMismatch

// ── 管理与巡检 ──
gocache.TableNames()             // ["products" "sessions" "users"]
gocache.TableCount()             // 3
gocache.DeleteTable("products")  // 移除该表并 Close，表名释放可复用
```

> 💡 表之间完全隔离：`users.Flush()` 不影响 `sessions`；对某张表调用 `Close()` 只停它自己的 janitor，不影响其他表。

## 🧭 API 一览

以下均为 `*Cache[K, V]` 的方法（`K comparable`，`V any`）。

### 构造与生命周期

| 方法 | 说明 | 使用场景 |
|---|---|---|
| `New(cfg *Config[K, V]) *Cache[K, V]` | 创建缓存；`nil` 配置 = 全默认 | 创建实例 |
| `Close()` | 停止后台 janitor，幂等；之后读写仍可用（过期退化为读时惰性清理） | 优雅关闭 / 热重建 |

### 写入

| 方法 | 说明 | 使用场景 |
|---|---|---|
| `Set(key K, value V, opts ...SetOption)` | 插入或覆盖；无选项 = 永不过期；覆盖触发 `OnReplaced` 并清零访问计数 | 常规写入 |
| `SetTTL(key K, value V, ttl time.Duration)` | 等价 `Set(key, value, WithTTL(ttl))`，绝对 TTL | 缓存 token、验证码等到期即失效的数据 |
| `SetSlidingTTL(key K, value V, ttl time.Duration)` | 等价 `Set(key, value, WithSlidingTTL(ttl))`，每次访问续期 | 会话、热点数据的“空闲即回收”语义 |
| `SetIfAbsent(key K, value V, opts ...SetOption) bool` | 不存在（或已过期）才插入 | 分布式锁的本地兜底、初始化一次的配置 |
| `GetOrSet(key K, value V, opts ...SetOption) (actual V, loaded bool)` | 存在返回现值（`loaded=true`），否则插入（`loaded=false`），sync.Map 语义 | 并发初始化共享单例 |
| `GetAndSet(key K, value V, opts ...SetOption) (old V, existed bool)` | 覆盖并返回旧值 | 读改写中的“写入并留痕” |
| `CompareAndSwap(key K, match func(oldValue V) bool, newValue V, opts ...SetOption) bool` | 当前值满足 `match` 才原子替换 | 状态机推进、并发安全的计数更新 |
| `SetMany(pairs map[K]V, opts ...SetOption)` | 批量插入，共享同一组选项 | 预热、批量灌入 |

### 读取

| 方法 | 说明 | 使用场景 |
|---|---|---|
| `Get(key K) (V, bool)` | 取值并刷新：续滑动 TTL、访问计数 +1、LRU 置顶 | 绝大多数读场景 |
| `Peek(key K) (V, bool)` | 只读，零副作用（不续期/不计数/不动 LRU） | 健康检查、监控采样 |
| `GetItem(key K) (Item[K, V], bool)` | 取值 + 完整元数据快照（见 [Item](#itemkt-vt)） | 需要访问次数、创建/过期时间等元数据 |
| `Exists(key K) bool` | 是否存在（已过期视为不存在），不刷新 | 存在性判断 |
| `KeepAlive(key K) bool` | 续滑动 TTL + 更新访问统计，不返回值 | 长任务处理中的人工心跳 |
| `GetMany(keys ...K) map[K]V` | 批量读取（计一次命中/未命中） | 一次取多个配置项 |
| `ExpireIn(key K) (time.Duration, bool)` | 剩余存活时间；不存在或永不过期时 `ok=false` | 展示“还剩多久过期”、续期策略决策 |

### 删除

| 方法 | 说明 | 使用场景 |
|---|---|---|
| `Delete(key K) bool` | 删除，返回是否存在 | 常规删除 |
| `GetAndDelete(key K) (V, bool)` | 删除并返回旧值 | 取走即删（如一次性任务领取） |
| `CompareAndDelete(key K, match func(oldValue V) bool) (V, bool)` | 值满足 `match` 才原子删除 | 只删“自己写入的那个值”（防误删他人更新） |
| `DeleteMany(keys ...K) int` | 批量删除，返回实际删除数 | 批量失效 |
| `Flush()` | 清空全部条目，移除回调以 `CauseFlushed` 触发 | 测试重置、配置热更新 |

### 加载（Singleflight）

| 方法 | 说明 | 使用场景 |
|---|---|---|
| `GetOrLoad(ctx context.Context, key K, args ...any) (V, error)` | 未命中时用 `Config.Loader` 加载并写回；同 key 并发 miss 合并为一次加载；nil Loader 返回 `ErrKeyNotFound` | 读穿透数据库的标准路径 |
| `ComputeIfAbsent(ctx context.Context, key K, fn func(ctx context.Context, key K) (V, error)) (V, error)` | 同上，但用调用方临时提供的 fn | 一次性的昂贵计算 |

### 回调

| 方法 | 说明 | 使用场景 |
|---|---|---|
| `SetRemoveCallback(key K, fn func(key K, value V, cause RemoveCause)) bool` | 给已有条目挂载移除回调（两步设置；条目不存在或已过期返回 false） | 按条目注册资源释放钩子 |

> 全局回调 `OnAdded` / `OnReplaced` / `OnRemoved` 在 [Config](#-配置参考) 中配置；**所有回调都在锁外执行**，可安全回调本缓存。

### 遍历与统计

| 方法 | 说明 | 使用场景 |
|---|---|---|
| `Len() int` / `Count() int` | 当前条目数（两者等价；含已过期未被 janitor 清理的条目） | 容量监控 |
| `Cost() int64` | 全部条目成本之和（`CostFunc` 口径） | 内存用量监控 |
| `Keys() []K` | 所有存活 key | 导出 key 列表 |
| `ForEach(fn func(key K, value V) bool)` | 遍历存活条目，返回 false 提前终止；fn 锁外执行 | 全量扫描、重建索引 |
| `All() iter.Seq2[K, V]` | range-over-func 迭代键值 | `for k, v := range c.All()` |
| `AllItems() iter.Seq2[K, Item[K, V]]` | 迭代完整元数据快照（不产生访问计数） | 无副作用的巡检 |
| `MostAccessed(n int) []Item[K, V]` | 按访问次数降序前 n 个；`n < 0` 返回全部 | 热点 key 分析 |
| `Stats() Stats` | 聚合统计快照（见 [Stats](#stats)） | 打点上报 |
| `ResetStats()` | 统计计数归零，数据不受影响 | 按时间窗统计 |

### 命名表（包级函数）

| 函数 | 说明 | 使用场景 |
|---|---|---|
| `GetOrCreate[K, V](name string, cfg *Config[K, V]) (*Cache[K, V], error)` | 按名取表，不存在则用 cfg 创建；类型不匹配返回 `ErrTypeMismatch` | 多模块共享同一缓存实例 |
| `Table[K, V](name string) (*Cache[K, V], error)` | 只取不建，不存在返回 `ErrTableNotFound` | 依赖已初始化的表 |
| `DeleteTable(name string) bool` | 移除并 Close 一张表，返回是否存在 | 表级下线 |
| `TableNames() []string` | 全部表名 | 运维排查 |
| `TableCount() int` | 注册表数量 | 运维排查 |

### Set 选项（`SetOption`）

| 函数 | 说明 |
|---|---|
| `WithTTL(ttl time.Duration)` | 绝对 TTL，写入后到期必失效，读不延期；`ttl <= 0` 永不过期 |
| `WithSlidingTTL(ttl time.Duration)` | 滑动 TTL，每次访问（Get/GetItem/KeepAlive）自动续期 |
| `WithCost(cost int64)` | 覆盖 `CostFunc`，指定本条目的内存成本 |

### RemoveCause（移除原因）

| 常量 | 含义 |
|---|---|
| `CauseRemoved` | 显式删除（Delete / GetAndDelete / CompareAndDelete） |
| `CauseExpired` | TTL 到期（janitor 或读时惰性清理） |
| `CauseEvicted` | 容量/内存超限被淘汰 |
| `CauseFlushed` | Flush 清空 |

### 类型参考

<details>
<summary><b>Item[K, V] —— 条目元数据快照</b></summary>

| 字段 | 类型 | 说明 |
|---|---|---|
| `Key` | `K` | 键 |
| `Value` | `V` | 值 |
| `CreatedOn` | `time.Time` | 写入时间 |
| `AccessedOn` | `time.Time` | 最后访问时间 |
| `AccessCount` | `int64` | 访问计数（Get/GetItem/KeepAlive 等 +1；覆盖写归零） |
| `LifeSpan` | `time.Duration` | 配置的 TTL；0 = 永不过期 |
| `Sliding` | `bool` | 是否滑动 TTL |
| `ExpireAt` | `time.Time` | 绝对过期时间点（零值 = 永不过期） |
| `Cost` | `int64` | 内存成本 |

</details>

<details>
<summary><b>Stats —— 统计快照</b></summary>

| 字段 | 说明 |
|---|---|
| `Entries` / `Cost` / `Shards` | 条目数 / 总成本 / 分片数 |
| `Hits` / `Misses` | 读命中 / 未命中 |
| `Sets` / `Updates` | 新插入 / 覆盖 |
| `Deletes` / `Expired` / `Evicted` / `Flushed` | 显式删除 / 过期 / 淘汰 / Flush |
| `Loads` / `LoadErrors` / `CoalescedLoads` | 加载成功 / 失败 / 被合并的并发加载 |
| `HitRate() float64` | `Hits / (Hits + Misses)`，无读时返回 0 |

</details>

<details>
<summary><b>Logger 与错误值</b></summary>

| 成员 | 说明 |
|---|---|
| `Logger` 接口 | `Debugf / Warnf / Errorf` 三个方法，可接 slog/zap/zerolog |
| `NopLogger` | 默认静默 |
| `NewStdLogger(*log.Logger)` | 适配标准库 logger |
| `NewWriterLogger(io.Writer)` | 写入任意 Writer |
| `ErrKeyNotFound` | key 不存在（GetOrLoad 未配置 Loader 时返回） |
| `ErrKeyNotFoundOrLoadable` | 预留错误值：key 不存在且无加载器（当前 `GetOrLoad` 返回 `ErrKeyNotFound`） |
| `ErrTypeMismatch` | 命名表类型冲突 |
| `ErrTableNotFound` | 命名表不存在 |

</details>

## ⚙️ 配置参考

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

## ⚠️ 语义说明

- 覆盖写触发 `OnReplaced` 而非 `OnAdded`；
- `Flush` 会触发移除回调（cause=flushed）；
- 加载器返回 `error` 时错误原样透传给调用方；
- 回调全部在锁外执行，杜绝持锁回调导致的重入死锁风险；
- 过期条目在 `SetIfAbsent`/`GetOrSet`/覆盖写时被视为“不存在”。

## 🧪 测试

```bash
go test -count=1 .                       # 单元测试
CGO_ENABLED=1 go test -race -count=3 .   # 竞态检测（需 C 编译器）
go test -run '^$' -bench . -benchmem .   # 基准（见 PERFORMANCE.md）
```

## 📄 性能

实测数据（串行/并发/分片扩展性/过期清理/与 cache2go 的逐项对比）见 [PERFORMANCE.md](PERFORMANCE.md)。

## License

MIT
