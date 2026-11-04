package gocache

import "time"

// Batch operations. Each one amortizes lock acquisition and function-call
// overhead across many keys; they are also simply convenient.

// GetMany returns values for all keys that are present and live. Missing
// and expired keys are simply absent from the result.
func (c *core[K, V]) GetMany(keys ...K) map[K]V {
	out := make(map[K]V, len(keys))
	now := c.now()
	for _, k := range keys {
		s, _ := c.shardFor(k)
		s.mu.Lock()
		if e, ok := s.items[k]; ok && !e.expiredAt(now) {
			out[k] = e.value
			e.touchRefresh(now)
			c.touchPolicyLocked(s, e, now)
			s.st.hits++
		} else {
			s.st.misses++
		}
		s.mu.Unlock()
	}
	return out
}

// SetMany inserts every pair with the same options (TTL etc.).
func (c *core[K, V]) SetMany(pairs map[K]V, opts ...SetOption) {
	p := newParams(opts)
	for k, v := range pairs {
		c.setWithParams(k, v, p)
	}
}

// DeleteMany removes every key and returns how many were present.
func (c *core[K, V]) DeleteMany(keys ...K) int {
	removed := 0
	for _, k := range keys {
		if c.Delete(k) {
			removed++
		}
	}
	return removed
}

// ExpireIn returns how long key would still live; ok is false when the
// key is absent or never expires. Handy for health checks and probes.
func (c *core[K, V]) ExpireIn(key K) (time.Duration, bool) {
	now := c.now()
	s, _ := c.shardFor(key)
	s.mu.Lock()
	e, ok := s.items[key]
	if !ok || e.expireAt == 0 || e.expiredAt(now) {
		s.mu.Unlock()
		return 0, false
	}
	d := time.Duration(e.expireAt - now)
	s.mu.Unlock()
	return d, true
}
