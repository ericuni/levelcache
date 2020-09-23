package levelcache

import (
	"context"
	"time"

	"github.com/go-redis/redis"
)

// Options options
type Options struct {
	LRUCacheOptions   *LRUCacheOptions
	RedisCacheOptions *RedisCacheOptions
	Loader            func(ctx context.Context, keys []string) (map[string]string, error)
}

// LRUCacheOptions lru cache options
type LRUCacheOptions struct {
	Size        int64 // items count
	Timeout     time.Duration
	MissTimeout time.Duration // if zero, do not cache empty result
}

// RedisCacheOptions redis cache options
type RedisCacheOptions struct {
	Client      *redis.Client
	Prefix      string // real key is prefix_${key}
	HardTimeout time.Duration
	SoftTimeout time.Duration // at least ms precision
	MissTimeout time.Duration
}
