// Package gocache is a high-performance, generic, sharded in-memory cache
// for Go.
//
// It provides named tables, sliding expiration, KeepAlive, data loaders,
// added/removed/expiring callbacks, MostAccessed, Foreach, Flush and
// per-table logging, plus the features expected from a contemporary local
// cache: compile-time type safety via generics,
// lock sharding, absolute and sliding TTL with jitter, capacity and memory
// limits with LRU/LFU eviction, singleflight protection against cache
// stampedes, context-aware loading, batch operations, range iterators,
// predicate-based compare-and-swap operations and rich runtime statistics.
//
// # Basic usage
//
//	c := gocache.New[string, int](nil)
//	c.Set("answer", 42, gocache.WithTTL(time.Minute))
//	v, ok := c.Get("answer")
//
// # Loading values safely
//
//	c := gocache.New[string, *db.Record](&gocache.Config[string, *db.Record]{
//		Loader:  loadRecordFromDB,
//		LoadTTL: 10 * time.Minute,
//	})
//	rec, err := c.GetOrLoad(ctx, "user:1")
//
// Concurrent misses for the same key are coalesced into a single loader
// call (singleflight), which prevents thundering-herd reloads.
//
// # Named tables
//
//	cache, err := gocache.GetOrCreate[string, user]("sessions", nil)
package gocache
