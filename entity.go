package entitystore

import (
	"time"

	"cloud.google.com/go/datastore"
)

type Entity interface {
	Key() *datastore.Key

	SetCreatedAt(t time.Time)
	CreatedAt() time.Time

	SetUpdatedAt(t time.Time)
	UpdatedAt() time.Time

	SetSchemaVersion(v int)
	SchemaVersion() int

	CurrentSchemaVersion() int
}
