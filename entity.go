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

	// SchemaVersion / CurrentSchemaVersion はスキーマ移行のための枠です。
	// Datastore には組み込みのマイグレーション機構が無いため、
	// エンティティにバージョンを持たせておかないと後からの移行が困難になります。
	// 本パッケージは自動マイグレーションは行わず、保存時に CurrentSchemaVersion を書き込むところまでを担います。
	SetSchemaVersion(v int)
	SchemaVersion() int
	CurrentSchemaVersion() int

	PrePutAction(ctx context.Context) error
}
