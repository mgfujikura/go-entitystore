package cachestore

import (
	"context"
	"errors"

	"cloud.google.com/go/datastore"
)

// ErrCacheSizeOver はキャッシュサイズが上限を超えた場合に返されるエラーです。
var ErrCacheSizeOver = errors.New("cachestore: cache size over")

// Cachestore はエンティティのキャッシュストアのインターフェースです。
// キャッシュは datastore.Key をキー、エンティティの datastore.Property のスライスを値として扱います。
type Cachestore interface {
	// GetEntities は指定されたキーのエンティティをキャッシュから取得します。
	GetEntities(context.Context, []datastore.Key) (map[datastore.Key][]datastore.Property, error)
	// SetEntities は指定されたエンティティをキャッシュに保存します。
	SetEntities(context.Context, map[datastore.Key][]datastore.Property) error
	// DeleteEntities は指定されたキーのエンティティをキャッシュから削除します。
	DeleteEntities(context.Context, []datastore.Key) error
}
