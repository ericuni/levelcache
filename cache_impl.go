package levelcache

import (
	"bytes"
	"context"
	"time"

	"github.com/ericuni/errs"
	"github.com/ericuni/glog"
	"github.com/ericuni/levelcache/model"
	"github.com/go-redis/redis"
	"github.com/golang/protobuf/proto"
	"github.com/karlseguin/ccache"
)

var (
	missBytes = []byte("")
)

// CacheImpl cache implementation
type CacheImpl struct {
	name    string
	options *Options

	lruData *ccache.Cache
}

func newCacheImpl(name string, options *Options) *CacheImpl {
	c := &CacheImpl{
		name:    name,
		options: options,
	}
	if options := options.LRUCacheOptions; options != nil {
		c.lruData = ccache.New(ccache.Configure().MaxSize(options.Size))
	}
	return c
}

// MGet .
func (cache *CacheImpl) MGet(ctx context.Context, keys []string) (map[string][]byte, map[string]bool, error) {
	if len(keys) == 0 {
		return nil, nil, nil
	}

	valuesMap := make(map[string][]byte, len(keys))
	validsMap := make(map[string]bool, len(keys))

	missKeys := cache.mGetFromLRUCache(ctx, keys, valuesMap, validsMap)
	if len(missKeys) == 0 {
		return valuesMap, validsMap, nil
	}

	keys = missKeys
	missKeys = cache.mGetFromRedisCache(ctx, keys, valuesMap, validsMap)
	if len(missKeys) == 0 {
		return valuesMap, validsMap, nil
	}

	keys = missKeys
	values, err := cache.options.Loader(ctx, keys)
	if err != nil {
		return valuesMap, validsMap, errs.Trace(err)
	}

	for k, v := range values {
		valuesMap[k] = v
		validsMap[k] = true
	}

	missKeys = nil
	for _, key := range keys {
		_, ok := values[key]
		if !ok {
			missKeys = append(missKeys, key)
		}
	}
	if err := cache.mSet(ctx, values, missKeys); err != nil {
		return valuesMap, validsMap, errs.Trace(err)
	}

	return valuesMap, validsMap, nil
}

func (cache *CacheImpl) mGetFromLRUCache(ctx context.Context, keys []string, valuesMap map[string][]byte,
	validsMap map[string]bool) []string {
	if cache.options.LRUCacheOptions == nil {
		return keys
	}

	var missKeys []string
	for _, key := range keys {
		item := cache.lruData.Get(key)
		if item != nil {
			bs, ok := item.Value().([]byte)
			if !ok {
				missKeys = append(missKeys, key)
				glog.Errorln("wrong data type")
				continue
			}

			// loader once missed, so we return like it missed, but if already expired, we need to try next level
			if bytes.Compare(bs, missBytes) == 0 {
				if item.Expired() {
					missKeys = append(missKeys, key)
				}
				continue
			}

			data := model.Data{}
			err := proto.Unmarshal(bs, &data)
			if err != nil {
				missKeys = append(missKeys, key)
				glog.Errorln("wrong data content")
				continue
			}

			valuesMap[key] = data.Raw
			if !item.Expired() {
				validsMap[key] = true
				continue
			}
		}
		missKeys = append(missKeys, key)
	}
	return missKeys
}

func (cache *CacheImpl) mGetFromRedisCache(ctx context.Context, keys []string, valuesMap map[string][]byte,
	validsMap map[string]bool) []string {
	options := cache.options.RedisCacheOptions

	if options == nil {
		return keys
	}

	var missKeys []string

	pipe := options.Client.Pipeline()
	defer pipe.Close()

	cmds := make([]*redis.StringCmd, 0, len(keys))
	for _, key := range keys {
		cmds = append(cmds, pipe.Get(cache.mkRedisKey(key)))
	}
	pipe.Exec()

	now := time.Now()
	for i, key := range keys {
		v, err := cmds[i].Bytes()
		if err != nil {
			missKeys = append(missKeys, key)
			continue
		}

		// loader miss
		if bytes.Compare(v, missBytes) == 0 {
			continue
		}

		data := model.Data{}
		err = proto.Unmarshal(v, &data)
		if err != nil {
			missKeys = append(missKeys, key)
			glog.Errorf("[%v] redis data format error", key)
			continue
		}

		if now.Sub(time.Unix(data.ModifyTime, 0)) <= options.SoftTimeout {
			valuesMap[key] = data.Raw
			validsMap[key] = true
			continue
		}

		// lrucache expired has higher priority over redis cache soft expired
		if _, ok := valuesMap[key]; !ok {
			valuesMap[key] = data.Raw
		}
		missKeys = append(missKeys, key)
	}
	return missKeys
}

// MSet .
func (cache *CacheImpl) MSet(ctx context.Context, kvs map[string][]byte) error {
	return cache.mSet(ctx, kvs, nil)
}

func (cache *CacheImpl) mSet(ctx context.Context, kvs map[string][]byte, missKeys []string) error {
	if len(kvs) == 0 && len(missKeys) == 0 {
		return nil
	}

	cache.mSetLRUCache(ctx, kvs, missKeys)

	if err := cache.mSetRedisCache(ctx, kvs, missKeys); err != nil {
		return errs.Trace(err)
	}

	return nil
}

func (cache *CacheImpl) mSetLRUCache(ctx context.Context, kvs map[string][]byte, missKeys []string) {
	options := cache.options.LRUCacheOptions
	if options == nil {
		return
	}

	now := time.Now().Unix()
	for k, v := range kvs {
		data := model.Data{
			Raw:        v,
			ModifyTime: now,
		}
		bs, _ := proto.Marshal(&data)
		cache.lruData.Set(k, bs, options.Timeout)
	}

	if options.MissTimeout == 0 {
		return
	}

	for _, key := range missKeys {
		cache.lruData.Set(key, missBytes, options.MissTimeout)
	}
}

func (cache *CacheImpl) mSetRedisCache(ctx context.Context, kvs map[string][]byte, missKeys []string) error {
	options := cache.options.RedisCacheOptions
	if options == nil {
		return nil
	}

	now := time.Now().Unix()
	pipe := options.Client.Pipeline()
	defer pipe.Close()
	var cmds []*redis.StatusCmd
	for k, v := range kvs {
		data := model.Data{
			Raw:        v,
			ModifyTime: now,
		}
		bs, _ := proto.Marshal(&data)
		cmds = append(cmds, pipe.Set(cache.mkRedisKey(k), bs, options.HardTimeout))
	}

	for i := 0; i < len(missKeys) && options.MissTimeout >= time.Millisecond; i++ {
		cmds = append(cmds, pipe.Set(cache.mkRedisKey(missKeys[i]), missBytes, options.MissTimeout))
	}

	_, err := pipe.Exec()
	if err != nil {
		return errs.Trace(err)
	}
	return nil

}

func (cache *CacheImpl) mkRedisKey(key string) string {
	if options := cache.options.RedisCacheOptions; options != nil {
		return options.Prefix + "_" + key
	}
	return key
}

// MDel .
func (cache *CacheImpl) MDel(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	if options := cache.options.LRUCacheOptions; options != nil {
		for _, key := range keys {
			cache.lruData.Delete(key)
		}
	}

	if options := cache.options.RedisCacheOptions; options != nil {
		var redisKeys []string
		for _, key := range keys {
			redisKeys = append(redisKeys, cache.mkRedisKey(key))
		}
		err := options.Client.Del(redisKeys...).Err()
		if err != nil {
			return errs.Trace(err)
		}
	}
	return nil
}
