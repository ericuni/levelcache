package levelcache_test

import (
	"context"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey"
	"github.com/ericuni/levelcache"
	"github.com/stretchr/testify/suite"
)

type LRUAndRedisCacheSuite struct {
	suite.Suite
	LevelCacheTest
	keys []string
}

func (s *LRUAndRedisCacheSuite) SetupSuite() {
	assert := s.Assert()

	s.ctx = context.Background()

	options := levelcache.Options{
		LRUCacheOptions: &levelcache.LRUCacheOptions{
			Size:        3,
			Timeout:     500 * time.Millisecond,
			MissTimeout: 100 * time.Millisecond,
		},
		RedisCacheOptions: &levelcache.RedisCacheOptions{
			Client:      getRedisClient(),
			Prefix:      "levelcache.test.lru_and_redis",
			HardTimeout: 2 * time.Second,
			SoftTimeout: 1 * time.Second,
			MissTimeout: 500 * time.Millisecond,
		},
		Loader: func(ctx context.Context, keys []string) (map[string][]byte, error) {
			s.hits = keys
			return nil, nil
		},
	}
	s.options = &options

	cache := levelcache.NewCache("lru_and_redis", s.options)
	assert.NotNil(cache)
	s.cache = cache

	s.keys = []string{"k1", "k2"}
}

func (s *LRUAndRedisCacheSuite) SetupTest() {
	assert := s.Assert()

	s.hits = nil

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
		s.hits = keys
		return map[string][]byte{key: []byte(value)}, nil
	})
	defer patches.Reset()

	t.Run("hit loader", func(t *testing.T) {
		s.hits = nil
		values, valids, err := s.get(key)
		assert.Equal([]string{key}, s.hits)

		assert.Nil(err)
		assert.Equal(value, values[key])
		assert.True(valids[key])
	})

	t.Run("hit cache", func(t *testing.T) {
		s.hits = nil
		values, valids, err := s.get(key)
		assert.Empty(s.hits)

		assert.Nil(err)
		assert.Equal(value, values[key])
		assert.True(valids[key])
	})

	t.Run("del cache", func(t *testing.T) {
		err := s.cache.MDel(s.ctx, []string{key})
		assert.Nil(err)
	})

	t.Run("hit loader agagin", func(t *testing.T) {
		s.hits = nil
		values, valids, err := s.get(key)
		assert.Equal([]string{key}, s.hits)

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
		s.hits = keys
		return map[string][]byte{key: []byte(value)}, nil
	})
	defer patches.Reset()

	t.Run("hit loader", func(t *testing.T) {
		s.hits = nil
		values, valids, err := s.get(key)
		assert.Equal([]string{key}, s.hits)

		assert.Nil(err)
		assert.Equal(value, values[key])
		assert.True(valids[key])
	})

	t.Run("hit lru cache", func(t *testing.T) {
		s.hits = nil
		values, valids, err := s.get(key)
		assert.Empty(s.hits)

		assert.Nil(err)
		assert.Equal(value, values[key])
		assert.True(valids[key])
	})

	t.Run("lru timeout and hit redis cache", func(t *testing.T) {
		time.Sleep(s.options.LRUCacheOptions.Timeout + 10*time.Millisecond)

		s.hits = nil
		values, valids, err := s.get(key)
		assert.Empty(s.hits)

		assert.Nil(err)
		assert.Equal(value, values[key])
		assert.True(valids[key])
	})

	t.Run("redis hard timeout and hit loader agagin", func(t *testing.T) {
		time.Sleep(s.options.RedisCacheOptions.HardTimeout + 10*time.Millisecond)

		s.hits = nil
		values, valids, err := s.get(key)
		assert.Equal([]string{key}, s.hits)

		assert.Nil(err)
		assert.Equal(value, values[key])
		assert.True(valids[key])
	})
}

func TestLRUAndRedisCache(t *testing.T) {
	suite.Run(t, new(LRUAndRedisCacheSuite))
}
