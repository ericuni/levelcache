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

type LRUCacheSuite struct {
	suite.Suite
	LevelCacheTest
}

func (s *LRUCacheSuite) SetupSuite() {
}

func (s *LRUCacheSuite) SetupTest() {
	assert := s.Assert()

	options := levelcache.Options{
		LRUCacheOptions: &levelcache.LRUCacheOptions{
			Size:        3,
			Timeout:     500 * time.Millisecond,
			MissTimeout: 100 * time.Millisecond,
		},
		Loader: func(ctx context.Context, keys []string) (map[string][]byte, error) {
			s.loaderRequestKeys = keys
			return nil, nil
		},
	}
	s.options = &options

	cache := levelcache.NewCache("levelcache.test.lru", s.options)
	assert.NotNil(cache)
	s.cache = cache

	s.loaderRequestKeys = nil
}

func (s *LRUCacheSuite) TestEmpty() {
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

func (s *LRUCacheSuite) TestHitLoader() {
	assert := s.Assert()
	t := s.T()

	key := "a"
	value := "va"

	t.Run("hittable loader", func(t *testing.T) {
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

		t.Run("hit cache", func(t *testing.T) {
			s.loaderRequestKeys = nil
			values, valids, err := s.get(key)
			assert.Empty(s.loaderRequestKeys)

			assert.Nil(err)
			assert.Equal(value, values[key])
			assert.True(valids[key])
		})

		t.Run("timeout and hit loader agagin", func(t *testing.T) {
			time.Sleep(s.options.LRUCacheOptions.Timeout + 10*time.Millisecond)

			s.loaderRequestKeys = nil
			values, valids, err := s.get(key)
			assert.Equal([]string{key}, s.loaderRequestKeys)

			assert.Nil(err)
			assert.Equal(value, values[key])
			assert.True(valids[key])
		})
	})

	t.Run("loader always misses", func(t *testing.T) {
		t.Run("timeout but loader miss and return expired value", func(t *testing.T) {
			time.Sleep(s.options.LRUCacheOptions.Timeout + 10*time.Millisecond)

			s.loaderRequestKeys = nil
			values, valids, err := s.get(key)
			assert.Equal([]string{key}, s.loaderRequestKeys)

			assert.Nil(err)
			assert.Equal(value, values[key])
			assert.False(valids[key])
		})
	})

	t.Run("loader error", func(t *testing.T) {
		patches := gomonkey.ApplyFunc(s.options.Loader, func(ctx context.Context, keys []string) (map[string][]byte,
			error) {
			s.loaderRequestKeys = keys
			return nil, errors.New("loader error")
		})
		defer patches.Reset()

		values, valids, err := s.get("new key")
		assert.NotNil(err)
		assert.Empty(values)
		assert.Empty(valids)
	})
}

func (s *LRUCacheSuite) TestMiss() {
	assert := s.Assert()
	t := s.T()

	key := "a"
	value := "va"

	t.Run("loader always misses", func(t *testing.T) {
		t.Run("loader miss", func(t *testing.T) {
			s.loaderRequestKeys = nil
			values, valids, err := s.get(key)
			assert.Equal([]string{key}, s.loaderRequestKeys)

			assert.Nil(err)
			assert.Empty(values)
			assert.Empty(valids)
		})

		t.Run("hit cache", func(t *testing.T) {
			s.loaderRequestKeys = nil
			values, valids, err := s.get(key)
			assert.Empty(s.loaderRequestKeys)

			assert.Nil(err)
			assert.Empty(values)
			assert.Empty(valids)
		})

		t.Run("timeout and arrive loader agagin but miss agagin", func(t *testing.T) {
			time.Sleep(s.options.LRUCacheOptions.MissTimeout + 10*time.Millisecond)

			s.loaderRequestKeys = nil
			values, valids, err := s.get(key)
			assert.Equal([]string{key}, s.loaderRequestKeys)

			assert.Nil(err)
			assert.Empty(values)
			assert.Empty(valids)
		})
	})

	t.Run("hittable loader", func(t *testing.T) {
		patches := gomonkey.ApplyFunc(s.options.Loader, func(ctx context.Context, keys []string) (map[string][]byte,
			error) {
			s.loaderRequestKeys = keys
			return map[string][]byte{key: []byte(value)}, nil
		})
		defer patches.Reset()

		t.Run("timeout and hit loader", func(t *testing.T) {
			time.Sleep(s.options.LRUCacheOptions.MissTimeout + 10*time.Millisecond)

			s.loaderRequestKeys = nil
			values, valids, err := s.get(key)
			assert.Equal([]string{key}, s.loaderRequestKeys)

			assert.Nil(err)
			assert.Equal(value, values[key])
			assert.True(valids[key])
		})
	})
}

func (s *LRUCacheSuite) TestLoaderPartMiss() {
	assert := s.Assert()
	t := s.T()

	k1 := "k1"
	v1 := "v1"
	k2 := "k2"

	patches := gomonkey.ApplyFunc(s.options.Loader, func(ctx context.Context, keys []string) (map[string][]byte, error) {
		s.loaderRequestKeys = keys
		return map[string][]byte{k1: []byte(v1)}, nil
	})
	defer patches.Reset()

	t.Run("load", func(t *testing.T) {
		s.loaderRequestKeys = nil
		values, valids, err := s.mget([]string{k1, k2})
		assert.Equal([]string{k1, k2}, s.loaderRequestKeys)

		assert.Nil(err)
		assert.Equal(v1, values[k1])
		assert.True(valids[k1])
		_, ok := values[k2]
		assert.False(ok)
	})

	t.Run("both hit cache including one miss cache", func(t *testing.T) {
		s.loaderRequestKeys = nil
		values, valids, err := s.mget([]string{k1, k2})
		assert.Empty(s.loaderRequestKeys)

		assert.Nil(err)
		assert.Equal(v1, values[k1])
		assert.True(valids[k1])
		_, ok := values[k2]
		assert.False(ok)
	})
}

func (s *LRUCacheSuite) TestFull() {
	assert := s.Assert()
	t := s.T()

	getValue := func(key string) string {
		return "value of " + key
	}

	patches := gomonkey.ApplyFunc(s.options.Loader, func(ctx context.Context, keys []string) (map[string][]byte, error) {
		s.loaderRequestKeys = keys
		values := make(map[string][]byte, len(keys))
		for _, key := range keys {
			values[key] = []byte(getValue(key))
		}
		return values, nil
	})
	defer patches.Reset()

	t.Run("fullfill cache", func(t *testing.T) {
		keys := []string{"a", "b", "c"}

		s.loaderRequestKeys = nil
		values, valids, err := s.mget(keys)
		assert.Equal(keys, s.loaderRequestKeys)

		assert.Nil(err)
		for _, key := range keys {
			assert.Equal(getValue(key), values[key])
			assert.True(valids[key])
		}
	})

	t.Run("get new keys", func(t *testing.T) {
		keys := []string{"xa", "xb", "xc"}

		s.loaderRequestKeys = nil
		values, valids, err := s.mget(keys)
		assert.Equal(keys, s.loaderRequestKeys)

		assert.Nil(err)
		for _, key := range keys {
			assert.Equal(getValue(key), values[key])
			assert.True(valids[key])
		}
	})

	// give lrucache async gc some time
	time.Sleep(10 * time.Millisecond)

	t.Run("get a evicted key", func(t *testing.T) {
		key := "a"

		s.loaderRequestKeys = nil
		values, valids, err := s.get(key)
		assert.Equal([]string{key}, s.loaderRequestKeys)

		assert.Nil(err)
		assert.Equal(getValue(key), values[key])
		assert.True(valids[key])
	})
}

func (s *LRUCacheSuite) TestMSet() {
	assert := s.Assert()
	t := s.T()

	options := levelcache.Options{
		LRUCacheOptions: &levelcache.LRUCacheOptions{
			Size:        3,
			Timeout:     500 * time.Millisecond,
			MissTimeout: 100 * time.Millisecond,
		},
	}
	cache := levelcache.NewCache("levelcache.test.lru.mset", &options)
	assert.NotNil(cache)
	s.cache = cache

	t.Run("hit", func(t *testing.T) {
		key := "a"
		value := "va"
		s.cache.MSet(s.ctx, map[string][]byte{key: []byte(value)})
		values, valids, err := s.get("a")
		assert.Nil(err)
		assert.Equal(value, values[key])
		assert.True(valids[key])
	})

	t.Run("miss", func(t *testing.T) {
		key := "none"
		values, valids, err := s.get(key)
		assert.Nil(err)
		_, ok := values[key]
		assert.False(ok)
		assert.False(valids[key])
	})
}

func (s *LRUCacheSuite) TestMDel() {
	assert := s.Assert()
	t := s.T()

	key := "a"
	value := "va"

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

func (s *LRUCacheSuite) TestCompression() {
	assert := s.Assert()

	options := s.options
	options.CompressionType = levelcache.CompressionType_Snappy
	cache := levelcache.NewCache("levelcache.test.lru.compression", options)
	assert.NotNil(cache)
	s.cache = cache

	key := "key"
	value := "bigvalue_xxxxxxxxxxxx_bigvalue"

	patches := gomonkey.ApplyFunc(s.options.Loader, func(ctx context.Context, keys []string) (map[string][]byte, error) {
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
	// hit cache
	valuesMap, validsMap, err = s.get(key)
	assert.Nil(err)
	assert.Equal(value, valuesMap[key])
	assert.True(validsMap[key])
	assert.Empty(s.loaderRequestKeys)
}

func TestLRUCache(t *testing.T) {
	suite.Run(t, new(LRUCacheSuite))
}
