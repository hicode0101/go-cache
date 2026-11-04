package gocache

import "sync"

// Named-table registry. Tables are strongly typed: the first
// GetOrCreate[K, V] call for a name fixes the table's types; later calls
// with different types fail with ErrTypeMismatch instead of panicking on
// a bad type assertion.

var (
	regMu    sync.Mutex
	regTable = map[string]any{}
)

// GetOrCreate returns the named table, creating it (with cfg) on first
// use. A nil cfg selects defaults. Subsequent calls with the same K/V
// return the same instance; the cfg of later calls is ignored.
func GetOrCreate[K comparable, V any](name string, cfg *Config[K, V]) (*Cache[K, V], error) {
	regMu.Lock()
	defer regMu.Unlock()
	if existing, ok := regTable[name]; ok {
		if tc, same := existing.(*Cache[K, V]); same {
			return tc, nil
		}
		return nil, ErrTypeMismatch
	}
	c := New(cfg)
	regTable[name] = c
	return c, nil
}

// Table returns an existing named table without creating it.
func Table[K comparable, V any](name string) (*Cache[K, V], error) {
	regMu.Lock()
	c, ok := regTable[name]
	regMu.Unlock()
	if !ok {
		return nil, ErrTableNotFound
	}
	tc, same := c.(*Cache[K, V])
	if !same {
		return nil, ErrTypeMismatch
	}
	return tc, nil
}

// DeleteTable removes and closes a named table. Holders of the old
// pointer keep a working cache (Close only stops background cleanup),
// but the name becomes free for reuse. Reports whether a table existed.
func DeleteTable(name string) bool {
	regMu.Lock()
	c, ok := regTable[name]
	if ok {
		delete(regTable, name)
	}
	regMu.Unlock()
	if ok {
		switch t := c.(type) {
		case interface{ Close() }:
			t.Close()
		}
	}
	return ok
}

// TableNames lists all registered table names.
func TableNames() []string {
	regMu.Lock()
	defer regMu.Unlock()
	names := make([]string, 0, len(regTable))
	for name := range regTable {
		names = append(names, name)
	}
	return names
}

// TableCount reports how many named tables are registered.
func TableCount() int {
	regMu.Lock()
	defer regMu.Unlock()
	return len(regTable)
}
