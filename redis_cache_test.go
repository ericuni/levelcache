package levelcache_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey"
	"github.com/ericuni/levelcache"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/suite"
)

type RedisCacheSuite struct {
	suite.Suite
	LevelCacheTest
	client *redis.Client
	keys   []string // will be deleted from redis before every test
}

func (s *RedisCacheSuite) SetupSuite() {
	s.client = getRedisClient()
}

func (s *RedisCacheSuite) SetupTest() {
	assert := s.Assert()
	t := s.T()

	ctx := context.Background()
	s.ctx = ctx

	options := levelcache.Options{
		RedisCacheOptions: &levelcache.RedisCacheOptions{
			Client:      s.client,
			Prefix:      "levelcache.test.redis",
			HardTimeout: 11 * time.Second,
			SoftTimeout: 10 * time.Second,
			MissTimeout: 500 * time.Millisecond,
		},
		Loader: func(ctx context.Context, keys []string) (map[string][]byte, error) {
			t.Logf("loader request: %v", keys)
			s.loaderRequestKeys = keys
			return nil, nil
		},
	}
	s.options = &options

	cache := levelcache.NewCache("levelcache.test.redis", s.options)
	assert.NotNil(cache)
	s.cache = cache

	s.loaderRequestKeys = nil
	s.keys = []string{"k1", "k2"}

	err := s.cache.MDel(s.ctx, s.keys)
	assert.Nil(err)
}

func (s *RedisCacheSuite) TearDownTest() {
}

func (s *RedisCacheSuite) TestMDel() {
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

func (s *RedisCacheSuite) TestEmpty() {
	assert := s.Assert()
	t := s.T()

	t.Run("nil keys", func(t *testing.T) {
		values, valids, err := s.cache.MGet(s.ctx, nil)
		assert.Nil(err)
		assert.Nil(values)
		assert.Nil(valids)
	})

	t.Run("empty keys", func(t *testing.T) {
		values, valids, err := s.cache.MGet(s.ctx, []string{})
		assert.Nil(err)
		assert.Nil(values)
		assert.Nil(valids)
	})

	patches := gomonkey.ApplyFunc(s.options.Loader, func(ctx context.Context, keys []string) (map[string][]byte, error) {
		return nil, errors.New("loader error")
	})
	defer patches.Reset()

	// empty keys would return early
	t.Run("loader error", func(t *testing.T) {
		values, valids, err := s.cache.MGet(s.ctx, []string{})
		assert.Nil(err)
		assert.Nil(values)
		assert.Nil(valids)
	})
}

func (s *RedisCacheSuite) TestHitLoader() {
	assert := s.Assert()
	t := s.T()

	key := s.keys[0]
	value := "value"

	t.Run("hittable loader", func(t *testing.T) {
		patches := gomonkey.ApplyFunc(s.options.Loader, func(ctx context.Context, keys []string) (map[string][]byte,
			error) {
			t.Logf("loader request: %v", keys)
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

		t.Run("soft timeout and hit loader agagin", func(t *testing.T) {
			time.Sleep(s.options.RedisCacheOptions.SoftTimeout + 10*time.Millisecond)

			s.loaderRequestKeys = nil
			values, valids, err := s.get(key)
			assert.Equal([]string{key}, s.loaderRequestKeys)

			assert.Nil(err)
			assert.Equal(value, values[key])
			assert.True(valids[key])
		})

		t.Run("hard timeout and hit loader agagin", func(t *testing.T) {
			time.Sleep(s.options.RedisCacheOptions.HardTimeout + 10*time.Millisecond)

			s.loaderRequestKeys = nil
			values, valids, err := s.get(key)
			assert.Equal([]string{key}, s.loaderRequestKeys)

			assert.Nil(err)
			assert.Equal(value, values[key])
			assert.True(valids[key])
		})
	})

	t.Run("loader error", func(t *testing.T) {
		patches := gomonkey.ApplyFunc(s.options.Loader, func(ctx context.Context, keys []string) (map[string][]byte,
			error) {
			s.loaderRequestKeys = keys
			return nil, errors.New("loader error")
		})
		defer patches.Reset()

		t.Run("soft timeout and loader error", func(t *testing.T) {
			time.Sleep(s.options.RedisCacheOptions.SoftTimeout + 10*time.Millisecond)

			s.loaderRequestKeys = nil
			values, valids, err := s.get(key)
			assert.Equal([]string{key}, s.loaderRequestKeys)

			assert.NotNil(err)
			assert.Equal(value, values[key])
			assert.False(valids[key])
		})

		t.Run("hard timeout and loader error", func(t *testing.T) {
			time.Sleep(s.options.RedisCacheOptions.HardTimeout + 10*time.Millisecond)

			s.loaderRequestKeys = nil
			values, valids, err := s.get(key)
			assert.Equal([]string{key}, s.loaderRequestKeys)

			assert.NotNil(err)
			assert.Empty(values)
			assert.Empty(valids)
		})
	})
}

func (s *RedisCacheSuite) TestMiss() {
	assert := s.Assert()
	t := s.T()

	key := s.keys[0]

	t.Run("loader miss", func(t *testing.T) {
		s.loaderRequestKeys = nil
		values, valids, err := s.get(key)
		assert.Equal([]string{key}, s.loaderRequestKeys)

		assert.Nil(err)
		assert.Empty(values)
		assert.Empty(valids)
	})

	waitAsyncRedis()

	t.Run("hit cache", func(t *testing.T) {
		s.loaderRequestKeys = nil
		values, valids, err := s.get(key)
		assert.Empty(s.loaderRequestKeys)

		assert.Nil(err)
		assert.Empty(values)
		assert.Empty(valids)
	})

	t.Run("miss timeout and loader miss agagin", func(t *testing.T) {
		time.Sleep(s.options.RedisCacheOptions.MissTimeout + 10*time.Millisecond)
		s.loaderRequestKeys = nil
		values, valids, err := s.get(key)
		assert.Equal([]string{key}, s.loaderRequestKeys)

		assert.Nil(err)
		assert.Empty(values)
		assert.Empty(valids)
	})
}

func (s *RedisCacheSuite) TestCompression() {
	assert := s.Assert()

	options := s.options
	options.CompressionType = levelcache.CompressionType_Snappy
	cache := levelcache.NewCache("redis_cache.test.redis.compression", options)
	assert.NotNil(cache)
	s.cache = cache

	key := "key"
	value := "bigvalue_xxxxxxxxxxxx_bigvalue"

	patches := gomonkey.ApplyFunc(s.options.Loader, func(ctx context.Context, keys []string) (map[string][]byte,
		error) {
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

	waitAsyncRedis()

	s.loaderRequestKeys = nil
	// hit cache
	valuesMap, validsMap, err = s.get(key)
	assert.Nil(err)
	assert.Equal(value, valuesMap[key])
	assert.True(validsMap[key])
	assert.Empty(s.loaderRequestKeys)
}

func TestRedisCache(t *testing.T) {
	suite.Run(t, new(RedisCacheSuite))
}

