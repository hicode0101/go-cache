package gocache

import (
	"context"
	"sync"
	"sync/atomic"
)

// loadGroup coalesces concurrent loads for the same key into a single
// execution — the "singleflight" pattern that prevents cache stampedes
// (thundering herds) when a hot key expires and hundreds of goroutines
// miss simultaneously.
//
// Implemented from scratch: the leader runs the loader, concurrent
// callers for the same key wait on a done channel and share the result,
// panics are propagated to every waiter, and each waiter's context
// cancellation is honored without disturbing the leader.
type loadGroup[K comparable, V any] struct {
	mu    sync.Mutex
	calls map[K]*loadCall[V]
}

type loadCall[V any] struct {
	done     chan struct{}
	val      V
	err      error
	panicked any
	dups     atomic.Int32
}

// do executes fn for key, merging concurrent calls. shared reports
// whether this caller joined an in-flight call instead of running fn.
// ctx only bounds the caller's own waiting; it does not cancel the
// leader's load.
func (g *loadGroup[K, V]) do(ctx context.Context, key K, fn func() (V, error)) (v V, err error, shared bool) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[K]*loadCall[V])
	}
	if call, ok := g.calls[key]; ok {
		call.dups.Add(1)
		g.mu.Unlock()

		select {
		case <-call.done:
		case <-ctx.Done():
			var zero V
			return zero, ctx.Err(), true
		}
		if call.panicked != nil {
			panic(call.panicked)
		}
		return call.val, call.err, true
	}

	call := &loadCall[V]{done: make(chan struct{})}
	g.calls[key] = call
	g.mu.Unlock()

	g.run(call, fn)
	close(call.done)

	g.mu.Lock()
	if g.calls[key] == call {
		delete(g.calls, key)
	}
	g.mu.Unlock()

	if call.panicked != nil {
		panic(call.panicked)
	}
	return call.val, call.err, false
}

func (g *loadGroup[K, V]) run(call *loadCall[V], fn func() (V, error)) {
	defer func() {
		if r := recover(); r != nil {
			call.panicked = r
		}
	}()
	call.val, call.err = fn()
}

// storeLoaded inserts a loader-produced value honoring LoadTTL.
func (c *core[K, V]) storeLoaded(key K, v V) {
	if ttl := c.cfg.LoadTTL; ttl > 0 {
		c.SetTTL(key, v, ttl)
	} else {
		c.Set(key, v)
	}
}

// GetOrLoad returns the cached value for key, or loads it with the
// configured Loader on a miss. Extra args are forwarded to the loader.
//
// Concurrent misses on the same key trigger exactly one loader call; the
// result (value, error, or panic) is shared by all waiters. A nil Loader
// yields ErrKeyNotFound.
func (c *core[K, V]) GetOrLoad(ctx context.Context, key K, args ...any) (V, error) {
	if v, ok := c.Get(key); ok {
		return v, nil
	}
	loader := c.cfg.Loader
	if loader == nil {
		var zero V
		return zero, ErrKeyNotFound
	}

	v, err, shared := c.flights.do(ctx, key, func() (V, error) {
		v, err := loader(ctx, key, args...)
		if err != nil {
			c.loadErrs.Add(1)
			c.cfg.Logger.Errorf("loader for key %v failed: %v", key, err)
			return v, err
		}
		c.loads.Add(1)
		c.storeLoaded(key, v)
		return v, nil
	})
	if shared {
		c.coalesced.Add(1)
	}
	return v, err
}

// ComputeIfAbsent is GetOrLoad with a per-call loader instead of the
// configured one. Useful for one-off expensive computations; the same
// stampede protection applies.
func (c *core[K, V]) ComputeIfAbsent(ctx context.Context, key K, fn func(ctx context.Context, key K) (V, error)) (V, error) {
	if v, ok := c.Get(key); ok {
		return v, nil
	}
	v, err, shared := c.flights.do(ctx, key, func() (V, error) {
		v, err := fn(ctx, key)
		if err != nil {
			c.loadErrs.Add(1)
			return v, err
		}
		c.loads.Add(1)
		c.storeLoaded(key, v)
		return v, nil
	})
	if shared {
		c.coalesced.Add(1)
	}
	return v, err
}
