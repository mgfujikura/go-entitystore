package entitystore

import (
	"context"
	"time"

	"cloud.google.com/go/datastore"
)

// Entity はentitystoreで扱うエンティティ定義の基底インターフェースです。
type Entity interface {
	Key() *datastore.Key

	SetUpdatedAt(t time.Time)
	UpdatedAt() time.Time

	SetSchemaVersion(v int)
	SchemaVersion() int

	CurrentSchemaVersion() int

	PrePutAction(ctx context.Context) error
}
