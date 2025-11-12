package cachestore

import (
	"context"

	"cloud.google.com/go/datastore"
)

// Nostore はキャッシュを使用しない Cachestore の実装です。
type Nostore struct {
	Cachestore
}

func (n Nostore) GetEntities(_ context.Context, _ []datastore.Key) (map[datastore.Key][]datastore.Property, error) {
	return make(map[datastore.Key][]datastore.Property), nil
}

func (n Nostore) SetEntities(_ context.Context, _ map[datastore.Key][]datastore.Property) error {
	// 何もしない
	return nil
}

func (n Nostore) DeleteEntities(_ context.Context, _ []datastore.Key) error {
	// 何もしない
	return nil
}
