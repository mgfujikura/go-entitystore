package entitystore

import (
	"errors"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/stretchr/testify/require"
)

func TestIsProblem(t *testing.T) {
	require.False(t, IsProblem(nil))
	require.False(t, IsProblem(datastore.ErrNoSuchEntity))
	require.False(t, IsProblem(datastore.MultiError{nil, datastore.ErrNoSuchEntity}))
	require.True(t, IsProblem(errors.New("boom")))
	require.True(t, IsProblem(datastore.MultiError{nil, errors.New("boom")}))
}

func TestPickUp_skipsNoSuchEntity(t *testing.T) {
	ents := []string{"a", "b", "c"}
	err := datastore.MultiError{nil, datastore.ErrNoSuchEntity, nil}
	got := PickUp(ents, err)
	require.Equal(t, []string{"a", "c"}, got)
}

func TestPickUp_noError(t *testing.T) {
	ents := []string{"a", "b"}
	got := PickUp(ents, nil)
	require.Equal(t, ents, got)
}

func TestWrapCacheInvalidate(t *testing.T) {
	require.Nil(t, wrapCacheInvalidate(nil))
	err := wrapCacheInvalidate(errors.New("cache delete failed"))
	require.ErrorIs(t, err, ErrCacheInvalidate)
	require.ErrorContains(t, err, "cache delete failed")
}

func TestAggregation_registersAliases(t *testing.T) {
	a := NewAggregation(NewQuery("TestEntity")).
		WithCount().
		WithAvg("Value").
		WithIntSum("Value").
		WithFloat64Sum("Value2")
	agg := a.(*aggregation)

	require.Equal(t, []string{"count", "isum_Value"}, agg.intKeys)
	require.Equal(t, []string{"avg_Value", "fsum_Value2"}, agg.floatKeys)
}
