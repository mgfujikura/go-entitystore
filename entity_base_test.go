package entitystore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEntityBase_CreatedAtColumn(t *testing.T) {
	e := &EntityBase{}
	require.Equal(t, time.Time{}, e.CreatedAt())
	now := time.Now()
	e.SetCreatedAt(now)
	require.Equal(t, now.Truncate(time.Microsecond), e.CreatedAt())
}

func TestEntityBase_UpdatedAtColumn(t *testing.T) {
	e := &EntityBase{}
	require.Equal(t, time.Time{}, e.UpdatedAt())
	now := time.Now()
	e.SetUpdatedAt(now)
	require.Equal(t, now.Truncate(time.Microsecond), e.UpdatedAt())
}

func TestEntityBase_SchemaVersionColumn(t *testing.T) {
	e := &EntityBase{}
	require.Equal(t, 0, e.SchemaVersion())
	e.SetSchemaVersion(1)
	require.Equal(t, 1, e.SchemaVersion())
}

func TestEntityBase_CurrentSchemaVersion(t *testing.T) {
	e := &EntityBase{}
	require.Equal(t, 0, e.CurrentSchemaVersion())
}
