package levelcache_test

import (
	"context"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey"
	"github.com/ericuni/levelcache"
	"github.com/stretchr/testify/suite"
)

type LRUCacheSuite struct {
	suite.Suite
	cache   levelcache.Cache
	ctx     context.Context
	options *levelcache.Options
}

func (s *LRUCacheSuite) SetupSuite() {
	options := levelcache.Options{
		LRUCacheOptions: &levelcache.LRUCacheOptions{
			Size:        3,
			Timeout:     500 * time.Millisecond,
			MissTimeout: 100 * time.Millisecond,
		},
		Loader: func(ctx context.Context, keys []string) (map[string][]byte, error) {
			return nil, nil
		},
	}
	s.options = &options
}

func (s *LRUCacheSuite) SetupTest() {
	assert := s.Assert()

	cache := levelcache.NewCache("lru", s.options)
	assert.NotNil(cache)
	s.cache = cache
}

func (s *LRUCacheSuite) get(key string) (map[string]string, map[string]bool, error) {
	return s.mget([]string{key})
}

func (s *LRUCacheSuite) mget(keys []string) (map[string]string, map[string]bool, error) {
	t := s.T()

	t.Logf("req: keys %v", keys)
	raw, valids, err := s.cache.MGet(s.ctx, keys)
	values := convert(raw)
	t.Logf("rsp: values %v, valids %v, err %v\n\n", values, valids, err)
	return values, valids, err
}

func (s *LRUCacheSuite) TestEmpty() {
	assert := s.Assert()
	t := s.T()

	t.Run("nil", func(t *testing.T) {
		values, valids, err := s.cache.MGet(s.ctx, nil)
		assert.Nil(err)
		assert.Nil(values)
		assert.Nil(valids)
	})

	t.Run("empty", func(t *testing.T) {
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

	patches := gomonkey.ApplyFunc(s.options.Loader, func(ctx context.Context, keys []string) (map[string][]byte, error) {
		t.Logf("hit loader: %v", keys)
		return map[string][]byte{key: []byte(value)}, nil
	})
	defer patches.Reset()

	t.Run("hit loader", func(t *testing.T) {
		values, valids, err := s.get(key)
		assert.Nil(err)
		assert.Equal(value, values[key])
		assert.True(valids[key])
	})

	t.Run("hit cache", func(t *testing.T) {
		values, valids, err := s.get(key)
		assert.Nil(err)
		assert.Equal(value, values[key])
		assert.True(valids[key])
	})

	t.Run("timeout and hit loader agagin", func(t *testing.T) {
		time.Sleep(s.options.LRUCacheOptions.Timeout + 10*time.Millisecond)

		values, valids, err := s.get(key)
		assert.Nil(err)
		assert.Equal(value, values[key])
		assert.True(valids[key])
	})
}

func (s *LRUCacheSuite) TestMiss() {
	assert := s.Assert()
	t := s.T()

	patches := gomonkey.ApplyFunc(s.options.Loader, func(ctx context.Context, keys []string) (map[string][]byte, error) {
		t.Logf("hit loader: %v", keys)
		return nil, nil
	})
	defer patches.Reset()

	key := "a"

	t.Run("loader miss", func(t *testing.T) {
		values, valids, err := s.get(key)
		assert.Nil(err)
		assert.Empty(values)
		assert.Empty(valids)
	})

	t.Run("hit cache", func(t *testing.T) {
		values, valids, err := s.get(key)
		assert.Nil(err)
		assert.Empty(values)
		assert.Empty(valids)
	})

	t.Run("timeout and hit loader agagin but miss agagin", func(t *testing.T) {
		time.Sleep(s.options.LRUCacheOptions.MissTimeout + 10*time.Millisecond)
		values, valids, err := s.get(key)
		assert.Nil(err)
		assert.Empty(values)
		assert.Empty(valids)
	})
}

func TestLRUCache(t *testing.T) {
	suite.Run(t, new(LRUCacheSuite))
}
