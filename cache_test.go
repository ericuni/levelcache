package levelcache_test

import (
	"context"
	"testing"
	"time"

	"github.com/ericuni/levelcache"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
)

var (
	client *redis.Client
	kvs    = map[string]string{"a": "va", "b": "vb", "c": "vc"}
	loader = func(ctx context.Context, keys []string) (map[string][]byte, error) {
		values := make(map[string][]byte, len(keys))
		for _, key := range keys {
			v, ok := kvs[key]
			if ok {
				values[key] = []byte(v)
			}
		}
		return values, nil
	}
)

func init() {
	client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	reset()
}

// go test -v -run LRUAndRedis
func TestCache_LRUAndRedis(t *testing.T) {
	defer reset()
	assert := assert.New(t)

	options := levelcache.Options{
		LRUCacheOptions: &levelcache.LRUCacheOptions{
			Size:        3,
			Timeout:     500 * time.Millisecond,
			MissTimeout: 100 * time.Millisecond,
		},
		RedisCacheOptions: &levelcache.RedisCacheOptions{
			Client:      client,
			Prefix:      "prefix",
			HardTimeout: 2 * time.Second,
			SoftTimeout: 1 * time.Second,
			MissTimeout: 200 * time.Millisecond,
		},
		Loader: func(ctx context.Context, keys []string) (map[string][]byte, error) {
			t.Logf("load keys: %v", keys)
			return loader(ctx, keys)
		},
	}

	cache := levelcache.NewCache("redis", &options)
	ctx := context.Background()

	mget := func(keys []string) (map[string]string, map[string]bool, error) {
		t.Logf("req: %v", keys)
		raw, valids, err := cache.MGet(ctx, keys)
		values := convert(raw)
		t.Logf("rsp: values %v, valids %v, err %v\n\n", values, valids, err)
		return values, valids, err
	}

	// hit loader
	{
		keys := []string{"a", "b", "c"}
		values, valids, err := mget(keys)
		assert.Nil(err)
		for _, key := range keys {
			assert.Equal(kvs[key], values[key])
			assert.True(valids[key])
		}
	}

	// hit lru cache
	{
		keys := []string{"a", "b", "c"}
		values, valids, err := mget(keys)
		assert.Nil(err)
		for _, key := range keys {
			assert.Equal(kvs[key], values[key])
			assert.True(valids[key])
		}
	}

	// sleep lru timeout and hit redis cache
	{
		t.Log("sleep lru timeout")
		time.Sleep(options.LRUCacheOptions.Timeout + 10*time.Millisecond)
		keys := []string{"a", "b", "c"}
		values, valids, err := mget(keys)
		assert.Nil(err)
		for _, key := range keys {
			assert.Equal(kvs[key], values[key])
			assert.True(valids[key])
		}
	}
}

func reset() {
	for k := range kvs {
		client.Del(k)
	}
	client.Del("d")
}
