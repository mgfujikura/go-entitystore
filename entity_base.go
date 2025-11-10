package entitystore

import "time"

type EntityBase struct {
	CreatedAtColumn     time.Time `datastore:"CreatedAt"`
	UpdatedAtColumn     time.Time `datastore:"UpdatedAt"`
	SchemaVersionColumn int       `datastore:"SchemaVersion"`
}

func (e *EntityBase) SetCreatedAt(t time.Time) {
	e.CreatedAtColumn = t.Truncate(time.Microsecond)
}

func (e *EntityBase) CreatedAt() time.Time {
	return e.CreatedAtColumn
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
