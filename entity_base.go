package entitystore

import (
	"context"
	"time"
)

// EntityBase は Entity インターフェースの基本実装を提供する構造体です。
// Key() メソッド以外の Entity インターフェースのメソッドを実装しており、
// 埋め込みによって利用できます。
//
// SchemaVersion はマイグレーション用の枠のみを提供します。
// 実体の移行処理はアプリケーション側で SchemaVersion と CurrentSchemaVersion を比較して行ってください。
type EntityBase struct {
	UpdatedAtColumn     time.Time `datastore:"UpdatedAt"`
	SchemaVersionColumn int       `datastore:"SchemaVersion"`
}

func (e *EntityBase) SetUpdatedAt(t time.Time) {
	e.UpdatedAtColumn = t.Truncate(time.Microsecond)
}

func (e *EntityBase) UpdatedAt() time.Time {
	return e.UpdatedAtColumn
}

func (e *EntityBase) SetSchemaVersion(v int) {
	e.SchemaVersionColumn = v
}

func (e *EntityBase) SchemaVersion() int {
	return e.SchemaVersionColumn
}

func (e *EntityBase) CurrentSchemaVersion() int {
	return 0
}

func (e *EntityBase) PrePutAction(_ context.Context) error {
	e.UpdatedAtColumn = Now().Truncate(time.Microsecond)
	e.SchemaVersionColumn = e.CurrentSchemaVersion()
	return nil
}
