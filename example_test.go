package gocache

import (
	"context"
	"fmt"
	"time"
)

// Example demonstrates a typical walkthrough of the API.
func Example() {
	// A standalone, type-safe cache: string keys, string values.
	cache := New[string, string](nil)
	defer cache.Close()

	// Put an item that expires 5 seconds after its last access
	// (sliding lifespan semantics).
	cache.SetSlidingTTL("someKey", "This is a test!", 5*time.Second)

	// Retrieve it.
	if v, ok := cache.Get("someKey"); ok {
		fmt.Println("Found value in cache:", v)
	}

	// Register callbacks (fire outside all cache locks).
	cache.SetRemoveCallback("someKey", func(k string, v string, cause RemoveCause) {
		fmt.Println("Deleting:", k, v, cause)
	})

	// Remove and wipe.
	cache.Delete("someKey")
	cache.Flush()

	// Output:
	// Found value in cache: This is a test!
	// Deleting: someKey This is a test! removed
}

// ExampleGetOrLoad shows stampede-safe lazy loading.
func ExampleCache_GetOrLoad() {
	type user struct{ Name string }

	cache := New[string, *user](&Config[string, *user]{
		Loader: func(ctx context.Context, key string, args ...any) (*user, error) {
			// imagine a database call here
			return &user{Name: "Alice"}, nil
		},
		LoadTTL: time.Minute,
	})
	defer cache.Close()

	u, err := cache.GetOrLoad(context.Background(), "user:1")
	if err != nil {
		fmt.Println("err:", err)
		return
	}
	fmt.Println(u.Name)

	// Output:
	// Alice
}
