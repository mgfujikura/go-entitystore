package cachestore

import (
	"context"

	"cloud.google.com/go/datastore"
)

type Nostore struct {
	Cachestore
}

func (n Nostore) GetEntities(_ context.Context, keys []datastore.Key) (map[datastore.Key][]datastore.Property, error) {
	// 空のmapを返す
	m := make(map[datastore.Key][]datastore.Property)
	for _, key := range keys {
		m[key] = nil
	}
	return m, nil
}

func (n Nostore) SetEntities(_ context.Context, _ map[datastore.Key][]datastore.Property) error {
	// 何もしない
	return nil
}

func (n Nostore) DeleteEntities(_ context.Context, _ []datastore.Key) error {
	// 何もしない
	return nil
}
