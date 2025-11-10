package aememcachestore

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"fmt"

	"cloud.google.com/go/datastore"
	"google.golang.org/appengine/memcache"

	"go.fujikura.biz/entitystore/cachestore"
)

var SizeLimit = 950 * 1024 // 950KB
var Prefix = "DatastoreCache:"

type Cachestore struct {
	cachestore.Cachestore
}

func NewCachestore() Cachestore {
	return Cachestore{}
}

func KeyHash(key datastore.Key) string {
	hash := md5.Sum([]byte(key.Encode()))
	return hex.EncodeToString(hash[:])
}

// 値はすべて gob でエンコードして、バイト数を計算し、サイズを超える場合はエラーを返す
// gobでエンコードするのは []property.Property

func (c Cachestore) GetEntities(ctx context.Context, keys []datastore.Key) (map[datastore.Key][]datastore.Property, error) {
	hashedKeys := make([]string, len(keys))
	keyMap := make(map[string]datastore.Key, len(keys))
	for i, k := range keys {
		h := KeyHash(k)
		hashedKeys[i] = Prefix + h
		keyMap[Prefix+h] = k
	}
	itemMap, err := memcache.GetMulti(ctx, hashedKeys)
	if err != nil {
		return nil, err
	}
	psMap := make(map[datastore.Key][]datastore.Property)
	for hk, item := range itemMap {
		buf := bytes.NewBuffer(item.Value)
		var ps []datastore.Property
		err := gob.NewDecoder(buf).Decode(&ps)
		if err != nil {
			return nil, fmt.Errorf("gob decode error for key %v: %w", keyMap[hk], err)
		}
		psMap[keyMap[hk]] = ps
	}
	return psMap, nil
}

func (c Cachestore) SetEntities(ctx context.Context, keyValues map[datastore.Key][]datastore.Property) error {
	items := make([]*memcache.Item, 0, len(keyValues))
	for key, ps := range keyValues {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(ps)
		if err != nil {
			return fmt.Errorf("encode error: %v", err)
		}
		if buf.Len() > SizeLimit {
			return cachestore.ErrCacheSizeOver
		}
		items = append(items, &memcache.Item{
			Key:   Prefix + KeyHash(key),
			Value: buf.Bytes(),
		})
	}
	return memcache.SetMulti(ctx, items)
}

func (c Cachestore) DeleteEntities(ctx context.Context, keys []datastore.Key) error {
	hashedKeys := make([]string, len(keys))
	for i, k := range keys {
		h := KeyHash(k)
		hashedKeys[i] = Prefix + h
	}
	return memcache.DeleteMulti(ctx, hashedKeys)
}
