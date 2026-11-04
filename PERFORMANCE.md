# go-cache 性能测试报告

> 对比基线：[muesli/cache2go](https://github.com/muesli/cache2go)（v0 master，2022-10-12）
> 报告日期：2026-11-04

## 1. 测试环境

| 项目 | 值 |
|---|---|
| CPU | Intel Core i7-12700H（14 核 20 线程，P 核 + E 核混合架构） |
| 操作系统 | Windows 11 (10.0.26100) / Git Bash |
| Go | go1.25.1 windows/amd64 |
| 测试方式 | `go test -bench . -benchmem -count=1`，默认 benchtime=1s |
| 并行基准 | `b.RunParallel`（GOMAXPROCS=20） |

**注意**：数值为单次运行结果，绝对值会随机器状态波动（本机测试期间波动 ±15%），但同一次运行内的**相对对比**是公平的——两侧在同一进程、同一负载下交替不可分地执行同一形状的工作负载。

## 2. 与 cache2go 的直接对比

两侧使用相同的数据类型（string/int 键、相同 value 载荷）、相同的键空间（65536 条预填充）。`Parallel*` 为 `b.RunParallel` 多核并发；`Mixed` 为 4 读 1 写混合负载。

| 基准 | cache2go | go-cache | 提升 | 分配对比 (cache2go → go-cache) |
|---|---:|---:|---:|---|
| Set（串行，string 键） | 691.7 ns/op | 392.5 ns/op | **1.76×** | 4 allocs → 3 allocs |
| Get 命中（串行，string 键） | 150.2 ns/op | 114.1 ns/op | **1.32×** | 21 B / 1 alloc → 5 B / 0 allocs |
| Get 命中（串行，int 键） | 74.4 ns/op | 48.1 ns/op | **1.55×** | 7 B / 0 → 0 B / 0 |
| SetSlidingTTL（串行，cache2go 的核心语义） | 709.1 ns/op | 470.2 ns/op | **1.51×** | 5 allocs → 2 allocs |
| **并发 Get（int 键）** | 56.5 ns/op | 35.0 ns/op | **1.61×** | 0 allocs（双方） |
| **并发 Set（int 键）** | 349.9 ns/op | 57.2 ns/op | **6.12×** | 171 B/2 → 35 B/1 |
| **并发 Set（string 键）** | 439.4 ns/op | 69.2 ns/op | **6.35×** | 187 B/3 → 44 B/2 |
| **并发 NotFoundAdd**（cache2go 自带基准形状） | 239.4 ns/op | 49.4 ns/op | **4.84×** | 40 B/2 → 33 B/1 |
| **并发混合读写（4:1）** | 497.3 ns/op | 42.6 ns/op | **11.67×** | 53 B/2 → 9 B/1 |

### 结论要点

1. **写路径是差距最大的地方**：cache2go 的整表写锁（`sync.RWMutex.Lock`）使并发 Set 完全串行；go-cache 分片后同一时刻可有 16 个分片并行写入，**并发 Set 快 6 倍以上**。
2. **混合负载差距最大（11.7×）**：真实业务缓存几乎都是读写混合。cache2go 的 RWMutex 在有写者竞争时读并发也会退化（写者饥饿控制 + 读计数器缓存行争用），go-cache 读写落在不同分片上互不干扰。
3. **读路径差距相对小**（1.3~1.6×）：cache2go 的 `Value()` 走 RLock，读本身可共享；差距主要来自泛型省掉的接口装箱/断言、每条目 RWMutex 的获取开销，以及更紧的临界区。
4. **零分配读**：go-cache 的 Get/Delete/Exists/KeepAlive 全路径 0 分配；cache2go 每次 `Value()` 返回 `*CacheItem` 需 1 次分配。

## 3. 分片数扩展性（本库自身）

`BenchmarkParallelGet/Set`，65536 条预填充，GOMAXPROCS=20：

| Shards | 并发 Get (ns/op) | 并发 Set (ns/op) |
|---:|---:|---:|
| 1 | 102.5 | 176.5 |
| 4 | 100.5 | 130.1 |
| 16 | 35.1 | 54.1 |
| 64 | 18.8 | 36.0 |

- 16 分片（默认）相对单锁：Get **2.9×**、Set **3.3×**；
- 64 分片：Get **5.5×**、Set **4.9×**；
- 20 线程 CPU 下 64 分片仍未饱和，多核机器建议 `Shards` 设为 CPU 数的 2~4 倍（向上取 2 的幂）。

## 4. 过期机制：O(n) 全表扫描 vs 堆 + 惰性

cache2go 每轮过期检查持有**整表写锁**遍历**全部**条目（包括永不过期的），复杂度 O(n)，期间阻塞该表所有读写；每条目还要各拿一次条目级 RWMutex。

go-cache 的方案：

- 每分片维护按过期时间排序的**最小堆**，sweep 只触碰真正到期的条目；
- 读路径 O(1) 惰性判断（`expireAt != 0 && now >= expireAt`），不依赖 janitor 也能保证正确性；
- janitor 睡眠时长自适应（取全局堆顶最小值），被插入更早过期时间时通过有缓冲 channel 精确唤醒；
- 堆记录带代数（generation）校验，滑动 TTL 刷新/覆盖写无需堆更新（陈旧记录弹出时直接丢弃）。

实测：**清理 1,000,000 条已到期条目耗时 23.2 ms（约 23 ns/条，0 分配，256 分片）**——各分片独立持锁清理，不阻塞其他分片读写。等价场景下 cache2go 需在单把整表锁内顺序扫描全部 100 万条目（按其单条 50~100ns 估算为 50~100ms+，且全程阻塞整表），并重复做这一切直到全部清完。

推论：当缓存中存在大量永不过期的条目时，cache2go 的每轮 `expirationCheck` 仍要全量扫描它们；go-cache 的堆方案对永不过期条目的维护成本为零（不进堆）。

## 5. 关键优化点清单

| # | 优化 | 相对 cache2go 的变化 | 效果佐证 |
|---:|---|---|---|
| 1 | **锁分片**（默认 16，可配，哈希取模） | 整表 1 把 RWMutex → 每分片独立 mutex | 并发 Set 6.1×、混合 11.7× |
| 2 | **移除条目级 RWMutex** | 每条目一把锁 → 分片锁统一保护，条目内字段无锁 | 内存更省，Get 1.55× |
| 3 | **泛型替代 interface{}** | 编译期类型安全；消除装箱、接口分发、类型断言 | int 键 Get 0 分配 |
| 4 | **运行时哈希做分片路由**（maphash.Comparable） | 支持任意 comparable 键，进程级随机化抗哈希碰撞注入 | — |
| 5 | **过期最小堆 + 惰性过期 + 精确唤醒** | 每轮 O(n) 全表扫描 → 只碰到期条目 | 1M 条清理 23ms，见 §4 |
| 6 | **堆代数校验** | 滑动 TTL 刷新/覆盖不修改堆（cache2go 每次写都要重排定时器） | SetWithTTL 330ns |
| 7 | **侵入式双向链表实现 LRU** | 无 container/list 节点分配；覆盖写原地复用条目 | 并发 Set 35B/op |
| 8 | **回调在锁外执行** | cache2go 部分回调持锁执行（有死锁/竞态隐患） | 正确性 + 无锁阻塞 |
| 9 | **统计按分片聚合** | 热计数器分散在各分片（写时无跨核缓存行争用），Stats() 汇总 | — |
| 10 | **Singleflight 合并并发加载** | cache2go 无此能力，热点 key 过期瞬间会打穿到后端 | 64 并发 miss 仅 1 次加载 |
| 11 | **时间戳用 int64 纳秒** | time.Time 每个占 24B × 2~3 处 → 8B | 条目更紧凑 |
| 12 | **全局计数器仅用于低频路径** | Len/Cost 用每分片原子变量求和，热路径无共享写 | — |

## 6. 本库自身基准摘录

| 基准 | 结果 |
|---|---:|
| Set（int 键，无 TTL） | 290.7 ns/op，210 B，2 allocs |
| Set + TTL | 330.1 ns/op，313 B，1 alloc |
| Get 命中 | 48.8 ns/op，**0 B，0 allocs** |
| Get 未命中 | 20.5 ns/op，0 allocs |
| Delete | 231.8 ns/op，0 allocs |
| 并发混合读写 | 40.8 ns/op，0 allocs |
| GetOrLoad 命中 / 未命中 | 46.9 / 550.2 ns/op |
| 并发 GetOrLoad | 38.9 ns/op |
| 100 键批量 Get | 3.77 µs（≈38 ns/键） |
| MostAccessed(100)（1 万条目） | 886 µs |
| ForEach（1 万条目） | 172 µs |
| string 键 Get | 57.6 ns/op |

## 7. 正确性验证

- `go test -race -count=3` 全部通过（Windows 下 -race 需 CGO + MinGW，本报告在装好 MinGW-w64 (GCC 16.2) 后执行）；
- 单元测试 40+ 个，覆盖：绝对/滑动 TTL、TTL 抖动、janitor 清理（含跨分片）、惰性过期、LRU/LFU/None 淘汰、内存上限、Singleflight（击穿/panic 传播/ctx 取消）、回调语义、命名表注册表、批量操作、迭代器、统计；
- 压力测试：6 goroutine × 500 轮混合读写 + 过期并发，重复 10 轮无失败。

开发过程中 -race 暴露并修复了一个真实 bug：janitor 若在首条写入前被调度，会按“空闲时长”进入长睡眠，而写入路径未发送唤醒信号，导致过期清理被延迟至多一个空闲周期——已在所有带 TTL 的写入点（Set/SetIfAbsent/GetOrSet/GetAndSet/CompareAndSwap，含覆盖写重排过期的分支）补上精确唤醒。该 bug 佐证了“写入时较早 deadline 必须唤醒 janitor”这一不变量的必要性。

## 8. 调优建议

1. **Shards**：默认 16 适合多数场景；持续写入 + ≥16 核时设为 CPU 数的 2~4 倍（自动向上取 2 的幂）。注意分片越多，`Len()`/`Stats()`/淘汰检查的求和成本线性上升。
2. **淘汰策略**：默认 LRU 足够；条目高度同质且写多读少时用 `PolicyNone`（零维护成本）；需要抗“一次性扫库污染”时用 `PolicyLFU`（Redis 风格 8 位对数计数 + 惰性衰减 + 采样淘汰，均为 O(1)-ish）。
3. **MaxMemory + CostFunc**：给出贴近真实的字节成本函数；不提供时每条目计 1（退化为条目数上限）。淘汰是全局目标、分片轮转执行，边界有少量超卖（每分片最多 1 条的容忍度）。
4. **ExpiryJitter**：批量写入 + 相同 TTL 的场景（如启动预热）建议设置（如 TTL 的 5~10%），避免同一时刻集中过期造成加载毛刺。
5. **CleanupInterval**：默认 1 分钟是“兜底上限”，不是清理延迟；真正决定清理及时性的是堆顶 + 精确唤醒。除非要省极多的定时器唤醒，不建议调小。
6. **Close**：进程常驻缓存可不调用（有 finalizer 兜底停 janitor）；热重建场景（如配置热更新）请显式 `Close()`。

