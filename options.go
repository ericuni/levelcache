package levelcache

import (
	"context"
	"time"

	"github.com/ericuni/errs"
	"github.com/go-redis/redis"
)

// Options options
type Options struct {
	LRUCacheOptions   *LRUCacheOptions
	RedisCacheOptions *RedisCacheOptions
	Loader            func(ctx context.Context, keys []string) (map[string][]byte, error)
	CompressionType   CompressionType
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

func (options *Options) isValid() error {
	if options == nil {
		return errs.New("options nil")
	}

	if options.LRUCacheOptions == nil && options.RedisCacheOptions == nil {
		return errs.New("both lrucache and rediscache options nil")
	}

	if err := options.LRUCacheOptions.isValid(); err != nil {
		return errs.Trace(err)
	}

	if err := options.RedisCacheOptions.isValid(); err != nil {
		return errs.Trace(err)
	}
	return nil
}

func (options *LRUCacheOptions) isValid() error {
	if options == nil {
		return nil
	}

	if options.Size <= 0 {
		return errs.New("lrucache size invalid")
	}
	if options.MissTimeout != 0 && options.MissTimeout < time.Millisecond {
		return errs.New("lrucache miss timeout at least 1ms")
	}
	if options.Timeout <= 0 || (options.MissTimeout != 0 && options.Timeout <= options.MissTimeout) {
		return errs.New("lrucache timeout invalid")
	}
	return nil
}

func (options *RedisCacheOptions) isValid() error {
	if options == nil {
		return nil
	}

	if options.Client == nil {
		return errs.New("redis client invalid")
	}
	if options.Prefix == "" {
		return errs.New("rediscache prefix invalid")
	}
	if options.MissTimeout != 0 && options.MissTimeout < time.Millisecond {
		return errs.New("rediscache miss timeout at least 1ms")
	}
	return nil
}
