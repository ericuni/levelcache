package levelcache

import (
	"bytes"
	"context"
	"time"

	"github.com/ericuni/errs"
	"github.com/go-redis/redis"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/karlseguin/ccache"
)

var (
	missBytes = []byte("")
)

// cacheImpl cache implementation
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

// MGet .
func (cache *cacheImpl) MGet(ctx context.Context, keys []string) (map[string][]byte, map[string]bool, error) {
	if len(keys) == 0 {
		return nil, nil, nil
	}

	valuesMap := make(map[string][]byte, len(keys))
	validsMap := make(map[string]bool, len(keys))

	lruMissKeys := cache.mGetFromLRUCache(ctx, keys, valuesMap, validsMap)
	if len(lruMissKeys) == 0 {
		return valuesMap, validsMap, nil
	}

	redisMissKeys := cache.mGetFromRedisCache(ctx, lruMissKeys, valuesMap, validsMap)

	// set redis to lru
	// if key is found in redis and value = missBytes, then key will not be added to missKeys, so key will appear in
	// hitRedisKeys, and key may(still in lru but expired) or may not be in valuesMap. if key already in valuesMap,
	// its lifetime will be extended, and if key not in, then it will be treated as miss key
	redisHitKeys := substract(lruMissKeys, redisMissKeys)
	if len(redisHitKeys) > 0 {
		redisValues := make(map[string][]byte, len(redisHitKeys))
		var emptyKeys []string
		for _, key := range redisHitKeys {
			if redisValue, ok := valuesMap[key]; ok {
				redisValues[key] = redisValue
			} else {
				emptyKeys = append(emptyKeys, key)
			}
		}
		cache.mSetLRUCache(ctx, redisValues, emptyKeys)
	}

	// hit redis all
	if len(redisMissKeys) == 0 {
		return valuesMap, validsMap, nil
	}

	if cache.options.Loader == nil {
		return valuesMap, validsMap, nil
	}

	values, err := cache.options.Loader(ctx, redisMissKeys)
	for k, v := range values {
		valuesMap[k] = v
		validsMap[k] = true
	}
	if err != nil {
		return valuesMap, validsMap, errs.Trace(err)
	}

	var loaderMissKeys []string
	for _, key := range redisMissKeys {
		_, ok := values[key]
		if !ok {
			loaderMissKeys = append(loaderMissKeys, key)
		}
	}
	if err := cache.mSet(ctx, values, loaderMissKeys); err != nil {
		return valuesMap, validsMap, errs.Trace(err)
	}

	return valuesMap, validsMap, nil
}

func (cache *cacheImpl) mGetFromLRUCache(ctx context.Context, keys []string, valuesMap map[string][]byte,
	validsMap map[string]bool) []string {
	if cache.options.LRUCacheOptions == nil || len(keys) == 0 {
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
			if bytes.Equal(bs, missBytes) {
				if item.Expired() {
					missKeys = append(missKeys, key)
				}
				continue
			}

			var data Data
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

func (cache *cacheImpl) mGetFromRedisCache(ctx context.Context, keys []string, valuesMap map[string][]byte,
	validsMap map[string]bool) []string {
	options := cache.options.RedisCacheOptions

	if options == nil || len(keys) == 0 {
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
		if bytes.Equal(v, missBytes) {
			continue
		}

		var data Data
		err = proto.Unmarshal(v, &data)
		if err != nil {
			missKeys = append(missKeys, key)
			glog.Errorf("[%v] redis data format error", key)
			continue
		}

		raw, err := decompress(data.CompressionType, data.Raw)
		if err != nil {
			glog.Errorf("%s redis %s decompress error +%v", cache.name, key, err)
		}

		if now.Sub(time.Unix(data.ModifyTime, 0)) <= options.SoftTimeout {
			valuesMap[key] = raw
			validsMap[key] = true
			continue
		}

		// lrucache expired has higher priority over redis cache soft expired
		if _, ok := valuesMap[key]; !ok {
			valuesMap[key] = raw
		}
		missKeys = append(missKeys, key)
	}
	return missKeys
}

// MSet .
func (cache *cacheImpl) MSet(ctx context.Context, kvs map[string][]byte) error {
	return cache.mSet(ctx, kvs, nil)
}

func (cache *cacheImpl) mSet(ctx context.Context, kvs map[string][]byte, missKeys []string) error {
	if len(kvs) == 0 && len(missKeys) == 0 {
		return nil
	}

	cache.mSetLRUCache(ctx, kvs, missKeys)

	if err := cache.mSetRedisCache(ctx, kvs, missKeys); err != nil {
		return errs.Trace(err)
	}

	return nil
}

func (cache *cacheImpl) mSetLRUCache(ctx context.Context, kvs map[string][]byte, missKeys []string) {
	options := cache.options.LRUCacheOptions
	if options == nil {
		return
	}

	now := time.Now().Unix()
	for k, v := range kvs {
		data := Data{
			Raw:             v,
			ModifyTime:      now,
			CompressionType: CompressionType_None,
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

func (cache *cacheImpl) mSetRedisCache(ctx context.Context, kvs map[string][]byte, missKeys []string) error {
	options := cache.options.RedisCacheOptions
	if options == nil {
		return nil
	}

	now := time.Now().Unix()
	pipe := options.Client.Pipeline()
	defer pipe.Close()
	for k, v := range kvs {
		data := Data{
			Raw:             compress(cache.options.CompressionType, v),
			ModifyTime:      now,
			CompressionType: cache.options.CompressionType,
		}
		bs, _ := proto.Marshal(&data)
		pipe.Set(cache.mkRedisKey(k), bs, options.HardTimeout)
	}

	if options.MissTimeout >= time.Millisecond {
		for _, key := range missKeys {
			pipe.Set(cache.mkRedisKey(key), missBytes, options.MissTimeout)
		}
	}

	_, err := pipe.Exec()
	if err != nil {
		return errs.Trace(err)
	}
	return nil

}

func (cache *cacheImpl) mkRedisKey(key string) string {
	if options := cache.options.RedisCacheOptions; options != nil {
		return options.Prefix + "_" + key
	}
	return key
}

// MDel .
func (cache *cacheImpl) MDel(ctx context.Context, keys []string) error {
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

func substract(x, y []string) []string {
	c := make(map[string]bool, len(y))
	for _, e := range y {
		c[e] = true
	}

	var diff []string
	for _, e := range x {
		if !c[e] {
			diff = append(diff, e)
		}
	}
	return diff
}

func compress(compressionType CompressionType, bs []byte) []byte {
	switch compressionType {
	case CompressionType_None:
		return bs
	case CompressionType_Snappy:
		return snappy.Encode(nil, bs)
	default:
		return bs
	}
}

func decompress(compressionType CompressionType, bs []byte) ([]byte, error) {
	switch compressionType {
	case CompressionType_None:
		return bs, nil
	case CompressionType_Snappy:
		decompressed, err := snappy.Decode(nil, bs)
		if err != nil {
			return nil, errs.Trace(err)
		}
		return decompressed, nil
	default:
		return nil, errs.New("unknown compress type %v", compressionType)
	}
}
