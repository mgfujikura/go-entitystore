package entitystore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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

func TestEntityBase_PrePutAction(t *testing.T) {
	Now = func() time.Time {
		return time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	}
	e := &EntityBase{}
	e.SchemaVersionColumn = 1

	require.Equal(t, time.Time{}, e.UpdatedAt())
	require.Equal(t, 1, e.SchemaVersion())

	err := e.PrePutAction(nil)
	require.NoError(t, err)

	require.Equal(t, Now().Truncate(time.Microsecond), e.UpdatedAt())
	require.Equal(t, 0, e.SchemaVersion())
}
