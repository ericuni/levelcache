package levelcache_test

import (
	"context"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey"
	"github.com/ericuni/levelcache"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/suite"
)

type LRUAndRedisCacheSuite struct {
	suite.Suite
	LevelCacheTest
	client *redis.Client
	keys   []string
}

func (s *LRUAndRedisCacheSuite) SetupSuite() {
	s.client = getRedisClient()
}

func (s *LRUAndRedisCacheSuite) SetupTest() {
	assert := s.Assert()

	ctx := context.Background()
	s.ctx = ctx

	options := levelcache.Options{
		LRUCacheOptions: &levelcache.LRUCacheOptions{
			Size:        3,
			Timeout:     500 * time.Millisecond,
			MissTimeout: 100 * time.Millisecond,
		},
		RedisCacheOptions: &levelcache.RedisCacheOptions{
			Client:      s.client,
			Prefix:      "levelcache.test.lru_and_redis",
			HardTimeout: 11 * time.Second,
			SoftTimeout: 10 * time.Second,
			MissTimeout: 500 * time.Millisecond,
		},
		Loader: func(ctx context.Context, keys []string) (map[string][]byte, error) {
			s.loaderRequestKeys = keys
			return nil, nil
		},
	}
	s.options = &options

	cache := levelcache.NewCache("levelcache.test.lru_and_redis", s.options)
	assert.NotNil(cache)
	s.cache = cache

	s.keys = []string{"k1", "k2"}
	s.loaderRequestKeys = nil

	err := s.cache.MDel(s.ctx, s.keys)
	assert.Nil(err)
}

func (s *LRUAndRedisCacheSuite) TestMDel() {
	assert := s.Assert()
	t := s.T()

	key := s.keys[0]
	value := "value"

	patches := gomonkey.ApplyFunc(s.options.Loader, func(ctx context.Context, keys []string) (map[string][]byte,
		error) {
		s.loaderRequestKeys = keys
		return map[string][]byte{key: []byte(value)}, nil
	})
	defer patches.Reset()

	t.Run("hit loader", func(t *testing.T) {
		s.loaderRequestKeys = nil
		values, valids, err := s.get(key)
		assert.Equal([]string{key}, s.loaderRequestKeys)

		assert.Nil(err)
		assert.Equal(value, values[key])
		assert.True(valids[key])
	})

	waitAsyncRedis()

	t.Run("hit cache", func(t *testing.T) {
		s.loaderRequestKeys = nil
		values, valids, err := s.get(key)
		assert.Empty(s.loaderRequestKeys)

		assert.Nil(err)
		assert.Equal(value, values[key])
		assert.True(valids[key])
	})

	t.Run("del cache", func(t *testing.T) {
		err := s.cache.MDel(s.ctx, []string{key})
		assert.Nil(err)
	})

	t.Run("hit loader agagin", func(t *testing.T) {
		s.loaderRequestKeys = nil
		values, valids, err := s.get(key)
		assert.Equal([]string{key}, s.loaderRequestKeys)

		assert.Nil(err)
		assert.Equal(value, values[key])
		assert.True(valids[key])
	})
}

func (s *LRUAndRedisCacheSuite) TestMGet() {
	assert := s.Assert()
	t := s.T()

	key := s.keys[0]
	value := "value"

	patches := gomonkey.ApplyFunc(s.options.Loader, func(ctx context.Context, keys []string) (map[string][]byte, error) {
		s.loaderRequestKeys = keys
		return map[string][]byte{key: []byte(value)}, nil
	})
	defer patches.Reset()

	t.Run("hit loader", func(t *testing.T) {
		s.loaderRequestKeys = nil
		values, valids, err := s.get(key)
		assert.Equal([]string{key}, s.loaderRequestKeys)

		assert.Nil(err)
		assert.Equal(value, values[key])
		assert.True(valids[key])
	})

	waitAsyncRedis()

	t.Run("hit lru cache", func(t *testing.T) {
		s.loaderRequestKeys = nil
		values, valids, err := s.get(key)
		assert.Empty(s.loaderRequestKeys)

		assert.Nil(err)
		assert.Equal(value, values[key])
		assert.True(valids[key])
	})

	t.Run("lru timeout and hit redis cache", func(t *testing.T) {
		time.Sleep(s.options.LRUCacheOptions.Timeout + 10*time.Millisecond)

		s.loaderRequestKeys = nil
		values, valids, err := s.get(key)
		assert.Empty(s.loaderRequestKeys)

		assert.Nil(err)
		assert.Equal(value, values[key])
		assert.True(valids[key])
	})

	t.Run("redis hard timeout and hit loader agagin", func(t *testing.T) {
		time.Sleep(s.options.RedisCacheOptions.HardTimeout + 10*time.Millisecond)

		s.loaderRequestKeys = nil
		values, valids, err := s.get(key)
		assert.Equal([]string{key}, s.loaderRequestKeys)

		assert.Nil(err)
		assert.Equal(value, values[key])
		assert.True(valids[key])
	})
}

func (s *LRUAndRedisCacheSuite) TestCompression() {
	assert := s.Assert()
	t := s.T()

	options := s.options
	options.CompressionType = levelcache.CompressionType_Snappy
	cache := levelcache.NewCache("levelcache.test.lru_and_redis.compression", options)
	assert.NotNil(cache)
	s.cache = cache

	key := "key"
	value := "bigvalue_xxxxxxxxxxxx_bigvalue"

	patches := gomonkey.ApplyFunc(s.options.Loader, func(ctx context.Context, keys []string) (map[string][]byte, error) {
		t.Logf("loader request: %v", keys)
		s.loaderRequestKeys = keys
		return map[string][]byte{key: []byte(value)}, nil
	})
	defer patches.Reset()

	// hit loader
	valuesMap, validsMap, err := s.get(key)
	assert.Nil(err)
	assert.Equal(value, valuesMap[key])
	assert.True(validsMap[key])
	assert.Equal([]string{key}, s.loaderRequestKeys)

	s.loaderRequestKeys = nil
	// hit lru cache
	valuesMap, validsMap, err = s.get(key)
	assert.Nil(err)
	assert.Equal(value, valuesMap[key])
	assert.True(validsMap[key])
	assert.Empty(s.loaderRequestKeys)

	time.Sleep(options.LRUCacheOptions.Timeout + 10*time.Millisecond)

	s.loaderRequestKeys = nil
	// hit redis cache
	valuesMap, validsMap, err = s.get(key)
	assert.Nil(err)
	assert.Equal(value, valuesMap[key])
	assert.True(validsMap[key])
	assert.Empty(s.loaderRequestKeys)
}

func TestLRUAndRedisCache(t *testing.T) {
	suite.Run(t, new(LRUAndRedisCacheSuite))
}

