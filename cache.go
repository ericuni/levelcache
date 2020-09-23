package levelcache

import (
	"context"
	"errors"
)

// Cache cache interface
type Cache interface {
	// if error is not nil, user decide whether to use expired values
	// second map, true for valid and false for expired
	MGet(ctx context.Context, keys []string) (map[string]string, map[string]bool, error)

	//  warm up cache
	MSet(ctx context.Context, kvs map[string]string) error
}

// NewCache create a new cache
// panic if options invalid
func NewCache(name string, options *Options) Cache {
	if name == "" || options == nil ||
		(options.LRUCacheOptions == nil && options.RedisCacheOptions == nil) ||
		options.Loader == nil {
		panic(errors.New("name or options invalid"))
	}

	if options := options.LRUCacheOptions; options != nil {
		if options.Size <= 0 {
			panic(errors.New("lrucache size invalid"))
		}
	}

	if options := options.RedisCacheOptions; options != nil {
		if options.Client == nil {
			panic(errors.New("redis client invalid"))
		}
		if options.Prefix == "" {
			panic(errors.New("redis prefix invalid"))
		}
	}
	return newCacheImpl(name, options)
}
