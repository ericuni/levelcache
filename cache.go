package levelcache

import (
	"context"
	"errors"
)

// Cache cache interface
type Cache interface {
	// if error is not nil, user decide whether to use expired values
	// second map, true for valid and false for expired
	MGet(ctx context.Context, keys []string) (map[string][]byte, map[string]bool, error)

	//  warm up cache
	MSet(ctx context.Context, kvs map[string][]byte) error

	// delete keys from cache, include local cache and redis cache.
	MDel(ctx context.Context, keys []string) error
}

// NewCache create a new cache
// panic if options invalid
func NewCache(name string, options *Options) Cache {
	if name == "" {
		panic(errors.New("name empty"))
	}

	if err := options.isValid(); err != nil {
		panic(err)
	}

	return newCacheImpl(name, options)
}
