package levelcache_test

import (
	"context"
	"errors"

	"github.com/ericuni/levelcache"
	"github.com/go-redis/redis"
)

type LevelCacheTest struct {
	cache   levelcache.Cache
	ctx     context.Context
	options *levelcache.Options
	hits    []string // keys which arrive at loader
}

func (s *LevelCacheTest) get(key string) (map[string]string, map[string]bool, error) {
	return s.mget([]string{key})
}

func (s *LevelCacheTest) mget(keys []string) (map[string]string, map[string]bool, error) {
	raw, valids, err := s.cache.MGet(s.ctx, keys)
	values := convert(raw)
	return values, valids, err
}

func convert(kvs map[string][]byte) map[string]string {
	res := make(map[string]string, len(kvs))
	for k, v := range kvs {
		res[k] = string(v)
	}
	return res
}

func getRedisClient() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	if client == nil {
		panic(errors.New("init redis client error"))
	}
	return client
}
