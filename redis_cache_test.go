package levelcache_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey"
	"github.com/ericuni/levelcache"
	"github.com/stretchr/testify/suite"
)

type RedisCacheSuite struct {
	suite.Suite
	LevelCacheTest
	keys []string // will be deleted from redis before every test
}

func (s *RedisCacheSuite) SetupSuite() {
	assert := s.Assert()

	s.ctx = context.Background()

	options := levelcache.Options{
		RedisCacheOptions: &levelcache.RedisCacheOptions{
			Client:      getRedisClient(),
			Prefix:      "levelcache.test.redis",
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

	cache := levelcache.NewCache("redis", s.options)
	assert.NotNil(cache)
	s.cache = cache

	s.keys = []string{"k1", "k2"}
}

func (s *RedisCacheSuite) SetupTest() {
	assert := s.Assert()

	s.hits = nil

	err := s.cache.MDel(s.ctx, s.keys)
	assert.Nil(err)
}

func (s *RedisCacheSuite) TestMDel() {
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

		// TODO: sometimes it would fail, do not know why
		// reids cache is set undoubtedly
		t.Run("hit cache", func(t *testing.T) {
			s.hits = nil
			values, valids, err := s.get(key)
			assert.Empty(s.hits)

			assert.Nil(err)
			assert.Equal(value, values[key])
			assert.True(valids[key])
		})

		t.Run("soft timeout and hit loader agagin", func(t *testing.T) {
			time.Sleep(s.options.RedisCacheOptions.SoftTimeout + 10*time.Millisecond)

			s.hits = nil
			values, valids, err := s.get(key)
			assert.Equal([]string{key}, s.hits)

			assert.Nil(err)
			assert.Equal(value, values[key])
			assert.True(valids[key])
		})

		t.Run("hard timeout and hit loader agagin", func(t *testing.T) {
			time.Sleep(s.options.RedisCacheOptions.HardTimeout + 10*time.Millisecond)

			s.hits = nil
			values, valids, err := s.get(key)
			assert.Equal([]string{key}, s.hits)

			assert.Nil(err)
			assert.Equal(value, values[key])
			assert.True(valids[key])
		})
	})

	t.Run("loader error", func(t *testing.T) {
		patches := gomonkey.ApplyFunc(s.options.Loader, func(ctx context.Context, keys []string) (map[string][]byte,
			error) {
			s.hits = keys
			return nil, errors.New("loader error")
		})
		defer patches.Reset()

		t.Run("soft timeout and loader error", func(t *testing.T) {
			time.Sleep(s.options.RedisCacheOptions.SoftTimeout + 10*time.Millisecond)

			s.hits = nil
			values, valids, err := s.get(key)
			assert.Equal([]string{key}, s.hits)

			assert.NotNil(err)
			assert.Equal(value, values[key])
			assert.False(valids[key])
		})

		t.Run("hard timeout and loader error", func(t *testing.T) {
			time.Sleep(s.options.RedisCacheOptions.HardTimeout + 10*time.Millisecond)

			s.hits = nil
			values, valids, err := s.get(key)
			assert.Equal([]string{key}, s.hits)

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
		s.hits = nil
		values, valids, err := s.get(key)
		assert.Equal([]string{key}, s.hits)

		assert.Nil(err)
		assert.Empty(values)
		assert.Empty(valids)
	})

	t.Run("hit cache", func(t *testing.T) {
		s.hits = nil
		values, valids, err := s.get(key)
		assert.Empty(s.hits)

		assert.Nil(err)
		assert.Empty(values)
		assert.Empty(valids)
	})

	t.Run("miss timeout and loader miss agagin", func(t *testing.T) {
		time.Sleep(s.options.RedisCacheOptions.MissTimeout + 10*time.Millisecond)
		s.hits = nil
		values, valids, err := s.get(key)
		assert.Equal([]string{key}, s.hits)

		assert.Nil(err)
		assert.Empty(values)
		assert.Empty(valids)
	})
}

func TestRedisCache(t *testing.T) {
	suite.Run(t, new(RedisCacheSuite))
}
