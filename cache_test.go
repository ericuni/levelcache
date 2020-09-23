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
	loader = func(ctx context.Context, keys []string) (map[string]string, error) {
		values := make(map[string]string, len(keys))
		for _, key := range keys {
			v, ok := kvs[key]
			if ok {
				values[key] = v
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

func TestCache_OnlyLRU(t *testing.T) {
	assert := assert.New(t)

	options := levelcache.Options{
		LRUCacheOptions: &levelcache.LRUCacheOptions{
			Size:        3,
			Timeout:     500 * time.Millisecond,
			MissTimeout: 100 * time.Millisecond,
		},
		Loader: func(ctx context.Context, keys []string) (map[string]string, error) {
			t.Logf("load keys: %v", keys)
			return loader(ctx, keys)
		},
	}

	cache := levelcache.NewCache("lru", &options)
	ctx := context.Background()

	mget := func(keys []string) (map[string]string, map[string]bool, error) {
		t.Logf("req: keys %v", keys)
		values, valids, err := cache.MGet(ctx, keys)
		t.Logf("rsp: values %v, valids %v, err %v\n\n", values, valids, err)
		return values, valids, err
	}

	// fill up cache, hit loader
	{
		keys := []string{"a", "b", "c"}
		values, valids, err := mget(keys)
		assert.Nil(err)
		for _, key := range keys {
			assert.Equal(kvs[key], values[key])
			assert.True(valids[key])
		}
	}

	// hit cache
	{
		keys := []string{"a", "b", "c"}
		_, _, err := mget(keys)
		assert.Nil(err)
	}

	// load miss key, this would replace a
	{
		keys := []string{"d"}
		_, _, err := mget(keys)
		assert.Nil(err)
	}

	// get d again, hit cache
	{
		keys := []string{"d"}
		_, _, err := mget(keys)
		assert.Nil(err)
	}

	// get d again, d expired
	{
		t.Log("sleep miss timeout")
		time.Sleep(options.LRUCacheOptions.MissTimeout + 10*time.Millisecond)
		keys := []string{"d"}
		_, _, err := mget(keys)
		assert.Nil(err)
	}

	// get a again, will cause its load
	{
		keys := []string{"a"}
		_, _, err := mget(keys)
		assert.Nil(err)
	}

	t.Log("sleep timeout")
	time.Sleep(options.LRUCacheOptions.Timeout + 10*time.Millisecond)
	{
		keys := []string{"a"}
		_, _, err := mget(keys)
		assert.Nil(err)
	}
}

func TestCache_OnlyRedis(t *testing.T) {
	defer reset()
	assert := assert.New(t)

	options := levelcache.Options{
		RedisCacheOptions: &levelcache.RedisCacheOptions{
			Client:      client,
			Prefix:      "prefix",
			HardTimeout: 2 * time.Second,
			SoftTimeout: 1 * time.Second,
			MissTimeout: 200 * time.Millisecond,
		},
		Loader: func(ctx context.Context, keys []string) (map[string]string, error) {
			t.Logf("load keys: %v", keys)
			return loader(ctx, keys)
		},
	}

	cache := levelcache.NewCache("redis", &options)
	ctx := context.Background()

	mget := func(keys []string) (map[string]string, map[string]bool, error) {
		t.Logf("req: %v", keys)
		values, valids, err := cache.MGet(ctx, keys)
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

	// hit cache
	{
		keys := []string{"a", "b", "c"}
		values, valids, err := mget(keys)
		assert.Nil(err)
		for _, key := range keys {
			assert.Equal(kvs[key], values[key])
			assert.True(valids[key])
		}
	}

	// get a new key, would hit loader, but miss
	{
		keys := []string{"d"}
		values, valids, err := mget(keys)
		assert.Nil(err)
		assert.Empty(values)
		assert.Empty(valids)
	}

	// get d again, hit cache
	{
		keys := []string{"d"}
		values, valids, err := mget(keys)
		assert.Nil(err)
		assert.Empty(values[keys[0]])
		assert.False(valids[keys[0]])
	}

	// miss timeout, will trigger load again
	{
		t.Log("sleep miss timeout")
		time.Sleep(options.RedisCacheOptions.MissTimeout + 10*time.Millisecond)
		keys := []string{"d"}
		values, valids, err := mget(keys)
		assert.Nil(err)
		assert.Empty(values)
		assert.Empty(valids)
	}

	// soft timeout, will trigger load again
	{
		t.Log("sleep soft timeout")
		time.Sleep(options.RedisCacheOptions.SoftTimeout + 10*time.Millisecond)
		keys := []string{"a"}
		values, valids, err := mget(keys)
		assert.Nil(err)
		key := keys[0]
		assert.Equal(kvs[key], values[key])
		assert.True(valids[key])
	}
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
		Loader: func(ctx context.Context, keys []string) (map[string]string, error) {
			t.Logf("load keys: %v", keys)
			return loader(ctx, keys)
		},
	}

	cache := levelcache.NewCache("redis", &options)
	ctx := context.Background()

	mget := func(keys []string) (map[string]string, map[string]bool, error) {
		t.Logf("req: %v", keys)
		values, valids, err := cache.MGet(ctx, keys)
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
