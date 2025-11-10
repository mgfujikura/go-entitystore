package cachestore

import (
	"context"
	"errors"

	"cloud.google.com/go/datastore"
)

var ErrCacheSizeOver = errors.New("cachestore: cache size over")

type Cachestore interface {
	GetEntities(context.Context, []datastore.Key) (map[datastore.Key][]datastore.Property, error)
	SetEntities(context.Context, map[datastore.Key][]datastore.Property) error
	DeleteEntities(context.Context, []datastore.Key) error
}
