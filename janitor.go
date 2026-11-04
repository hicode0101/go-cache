package gocache

import "time"

// janitorLoop proactively removes expired entries.
//
// Each shard keeps a min-heap of pending expirations, so a sweep only
// touches entries that are actually due: O(log n) per expired item, O(1)
// when nothing expired — unlike a whole-table scan, which is O(n) per
// sweep and blocks all readers and writers while it runs. The sleep
// duration is self-adjusting, derived from heap tops without scanning.
func (c *core[K, V]) janitorLoop() {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	for {
		d := c.nextNap()
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(d)

		select {
		case <-c.stopCh:
			return
		case <-c.wakeCh:
		case <-timer.C:
		}

		c.sweepAll()
	}
}

// nextNap computes how long the janitor should sleep:
//   - the nearest pending expiration, if any (precise wake-up);
//   - otherwise a long idle sleep (nothing to do);
//   - always capped by CleanupInterval as a safety margin, so a missed
//     wake signal can delay cleanup by at most that long.
func (c *core[K, V]) nextNap() time.Duration {
	now := c.now()
	nearest := int64(0)
	for _, s := range c.shards {
		if ne := s.nextExpire.Load(); ne > 0 && (nearest == 0 || ne < nearest) {
			nearest = ne
		}
	}
	cap := int64(c.cfg.CleanupInterval)
	if nearest == 0 {
		// idle: no pending expirations anywhere; a long sleep costs
		// nothing and the wake channel covers earlier insertions.
		return time.Duration(cap) * 120
	}
	d := nearest - now
	if d < 0 {
		d = 0
	}
	if d > cap {
		d = cap
	}
	return time.Duration(d)
}

// sweepAll sweeps every shard once, firing removal callbacks outside the
// shard locks.
func (c *core[K, V]) sweepAll() {
	for _, s := range c.shards {
		var fires []func()
		s.mu.Lock()
		c.sweepShardLocked(s, &fires)
		s.mu.Unlock()
		runFires(fires)
	}
}

// sweepShardLocked pops due (and stale) records from the shard's expiry
// heap. Caller holds s.mu.
func (c *core[K, V]) sweepShardLocked(s *shard[K, V], fires *[]func()) {
	now := c.now()
	for {
		top, ok := s.exp.peekMin()
		if !ok || top.at > now {
			break
		}
		s.exp.popMin()
		// Stale record? The entry was replaced or removed since the
		// record was pushed; its generation moved on.
		if top.e.gen.Load() != top.gen {
			continue
		}
		if top.e.expireAt == 0 {
			continue // became non-expiring (defensive)
		}
		if top.e.expireAt > now {
			// Sliding refresh moved the deadline later; refile with the
			// current deadline.
			s.exp.push(expEntry[K, V]{at: top.e.expireAt, gen: top.gen, e: top.e})
			continue
		}
		c.removeLocked(s, top.e, CauseExpired, fires)
	}
	s.recalcNextExpire()
}
