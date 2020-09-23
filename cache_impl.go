package levelcache

import (
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
	var missKeys []string

	if cache.options.LRUCacheOptions != nil {
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
				if len(bs) == 0 {
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
	} else {
		missKeys = keys
	}
	if len(missKeys) == 0 {
		return valuesMap, validsMap, nil
	}

	if cache.options.RedisCacheOptions != nil {
		options := cache.options.RedisCacheOptions
		keys = missKeys
		missKeys = nil

		func() {
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
				if v == "" {
					continue
				}

				data := model.Data{}
				err = proto.Unmarshal(toReadOnlyBytes(v), &data)
				if err != nil {
					missKeys = append(missKeys, key)
					glog.Errorf("[%v] redis data format error", key)
					continue
				}

				if now.Sub(time.Unix(data.ModifyTimeMs/1e3, (data.ModifyTimeMs%1e3)*1e6)) <= options.SoftTimeout {
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
		}()
	}
	if len(missKeys) == 0 {
		return valuesMap, validsMap, nil
	}

	keys = missKeys
	missKeys = nil
	values, err := cache.options.Loader(ctx, keys)
	if err != nil {
		return valuesMap, validsMap, errs.Trace(err)
	}

	for k, v := range values {
		valuesMap[k] = v
		validsMap[k] = true
	}

	if err := cache.MSet(ctx, values); err != nil {
		return valuesMap, validsMap, errs.Trace(err)
	}

	for _, key := range keys {
		_, ok := values[key]
		if !ok {
			missKeys = append(missKeys, key)
		}
	}
	if err := cache.msetMiss(ctx, missKeys); err != nil {
		return valuesMap, validsMap, errs.Trace(err)
	}

	return valuesMap, validsMap, nil
}

func (cache *cacheImpl) MSet(ctx context.Context, kvs map[string]string) error {
	return cache.mSet(ctx, kvs, false)
}

func (cache *cacheImpl) mSet(ctx context.Context, kvs map[string]string, isMiss bool) error {
	if len(kvs) == 0 {
		return nil
	}

	modifyTimeMs := time.Now().UnixNano() / 1e6

	if options := cache.options.LRUCacheOptions; options != nil {
		func() {
			if isMiss && options.MissTimeout.Milliseconds() <= 0 {
				return
			}

			for k, v := range kvs {
				if isMiss {
					cache.lruData.Set(k, missBytes, options.MissTimeout)
				} else {
					data := model.Data{
						Raw:          v,
						ModifyTimeMs: modifyTimeMs,
					}
					bs, _ := proto.Marshal(&data)
					cache.lruData.Set(k, bs, options.Timeout)
				}
			}
		}()
	}

	if options := cache.options.RedisCacheOptions; options != nil {
		err := func() error {
			if isMiss && options.MissTimeout.Milliseconds() <= 0 {
				return nil
			}

			pipe := options.Client.Pipeline()
			defer pipe.Close()
			var cmds []*redis.StatusCmd
			for k, v := range kvs {
				if isMiss {
					cmds = append(cmds, pipe.Set(cache.mkRedisKey(k), missBytes, options.MissTimeout))
				} else {
					data := model.Data{
						Raw:          v,
						ModifyTimeMs: modifyTimeMs,
					}
					bs, _ := proto.Marshal(&data)
					cmds = append(cmds, pipe.Set(cache.mkRedisKey(k), bs, options.HardTimeout))
				}
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

func (cache *cacheImpl) msetMiss(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	kvs := make(map[string]string, len(keys))
	for _, key := range keys {
		kvs[key] = ""
	}
	return cache.mSet(ctx, kvs, true)
}

func (cache *cacheImpl) mkRedisKey(key string) string {
	if options := cache.options.RedisCacheOptions; options != nil {
		return options.Prefix + "_" + key
	}
	return key
}
