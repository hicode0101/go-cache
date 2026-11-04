package gocache

import "errors"

var (
	// ErrKeyNotFound is returned when a key cannot be found in the cache.
	ErrKeyNotFound = errors.New("key not found in cache")

	// ErrKeyNotFoundOrLoadable is returned by GetOrLoad when the key is
	// missing and no loader is configured.
	ErrKeyNotFoundOrLoadable = errors.New("key not found and no loader configured")

	// ErrTypeMismatch is returned by GetOrCreate when a table with the
	// requested name already exists but with different key/value types.
	ErrTypeMismatch = errors.New("table exists with a different key/value type")

	// ErrTableNotFound is returned by table lookup helpers when the
	// requested named table does not exist.
	ErrTableNotFound = errors.New("table not found")
)
