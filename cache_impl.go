package levelcache

import (
	"bytes"
	"context"
	"time"

	"github.com/ericuni/errs"
	"github.com/ericuni/glog"
	"github.com/ericuni/gocode/common/str"
	"github.com/ericuni/levelcache/model"
	"github.com/go-redis/redis"
	"github.com/golang/protobuf/proto"
	"github.com/karlseguin/ccache"
)

var (
	missBytes = []byte("")
)

type cacheImpl struct {
	name    string
	options *Options

	lruData *ccache.Cache
}

func newCacheImpl(name string, options *Options) *cacheImpl {
	c := &cacheImpl{
		name:    name,
		options: options,
	}
	if options := options.LRUCacheOptions; options != nil {
		c.lruData = ccache.New(ccache.Configure().MaxSize(options.Size))
	}
	return c
}

func (cache *cacheImpl) MGet(ctx context.Context, keys []string) (map[string]string, map[string]bool, error) {
	if len(keys) == 0 {
		return nil, nil, nil
	}

	valuesMap := make(map[string]string, len(keys))
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

func (cache *cacheImpl) mGetFromLRUCache(ctx context.Context, keys []string, valuesMap map[string]string,
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

			// loader miss
			if bytes.Compare(bs, missBytes) == 0 {
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

func (cache *cacheImpl) mGetFromRedisCache(ctx context.Context, keys []string, valuesMap map[string]string,
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
		v, err := cmds[i].Result()
		if err != nil {
			missKeys = append(missKeys, key)
			continue
		}

		// loader miss
		if bytes.Compare(str.ToReadOnlyBytes(v), missBytes) == 0 {
			continue
		}

		data := model.Data{}
		err = proto.Unmarshal(toReadOnlyBytes(v), &data)
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

func (cache *cacheImpl) MSet(ctx context.Context, kvs map[string]string) error {
	return cache.mSet(ctx, kvs, nil)
}

func (cache *cacheImpl) mSet(ctx context.Context, kvs map[string]string, missKeys []string) error {
	if len(kvs) == 0 && len(missKeys) == 0 {
		return nil
	}

	modifyTimeMs := time.Now().Unix()

	if options := cache.options.LRUCacheOptions; options != nil {
		for k, v := range kvs {
			data := model.Data{
				Raw:        v,
				ModifyTime: modifyTimeMs,
			}
			bs, _ := proto.Marshal(&data)
			cache.lruData.Set(k, bs, options.Timeout)
		}

		for i := 0; i < len(missKeys) && options.MissTimeout.Milliseconds() > 0; i++ {
			cache.lruData.Set(missKeys[i], missBytes, options.MissTimeout)
		}
	}

	if options := cache.options.RedisCacheOptions; options != nil {
		err := func() error {
			pipe := options.Client.Pipeline()
			defer pipe.Close()
			var cmds []*redis.StatusCmd
			for k, v := range kvs {
				data := model.Data{
					Raw:        v,
					ModifyTime: modifyTimeMs,
				}
				bs, _ := proto.Marshal(&data)
				cmds = append(cmds, pipe.Set(cache.mkRedisKey(k), bs, options.HardTimeout))
			}

			for i := 0; i < len(missKeys) && options.MissTimeout >= time.Millisecond; i++ {
				cmds = append(cmds, pipe.Set(cache.mkRedisKey(missKeys[i]), missBytes, options.HardTimeout))
			}

			_, err := pipe.Exec()
			if err != nil {
				return errs.Trace(err)
			}
			return nil
		}()
		if err != nil {
			return errs.Trace(err)
		}
	}
	return nil
}

func (cache *cacheImpl) mkRedisKey(key string) string {
	if options := cache.options.RedisCacheOptions; options != nil {
		return options.Prefix + "_" + key
	}
	return key
}
