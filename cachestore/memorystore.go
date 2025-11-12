package cachestore

import (
	"context"

	"cloud.google.com/go/datastore"
)

// Memorystore はメモリ上にエンティティをキャッシュする Cachestore の実装です。
// テスト用途など、一時的なキャッシュが必要な場合に使用します。
// Goルーチンセーフではありません。
type Memorystore struct {
	Cache map[datastore.Key][]datastore.Property
	Cachestore
}

func (m *Memorystore) GetEntities(_ context.Context, keys []datastore.Key) (map[datastore.Key][]datastore.Property, error) {
	result := make(map[datastore.Key][]datastore.Property)
	for _, key := range keys {
		if props, ok := m.Cache[key]; ok {
			result[key] = props
		}
	}
	return result, nil
}

func (m *Memorystore) SetEntities(_ context.Context, keyValues map[datastore.Key][]datastore.Property) error {
	if m.Cache == nil {
		m.Cache = make(map[datastore.Key][]datastore.Property)
	}
	for key, value := range keyValues {
		m.Cache[key] = value
	}
	return nil
}

func (m *Memorystore) DeleteEntities(_ context.Context, keys []datastore.Key) error {
	for _, key := range keys {
		delete(m.Cache, key)
	}
	return nil
}
