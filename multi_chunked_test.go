package entitystore

import (
	"errors"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/stretchr/testify/require"
)

func TestChunkedMulti_empty(t *testing.T) {
	calls := 0
	err := chunkedMulti(0, 1000, func(start, end int) error {
		calls++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 0, calls)
}

func TestChunkedMulti_splitsByLimit(t *testing.T) {
	var ranges [][2]int
	err := chunkedMulti(2500, 1000, func(start, end int) error {
		ranges = append(ranges, [2]int{start, end})
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, [][2]int{{0, 1000}, {1000, 2000}, {2000, 2500}}, ranges)
}

func TestChunkedMulti_singleChunkWhenWithinLimit(t *testing.T) {
	var ranges [][2]int
	err := chunkedMulti(500, 1000, func(start, end int) error {
		ranges = append(ranges, [2]int{start, end})
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, [][2]int{{0, 500}}, ranges)
}

func TestChunkedMulti_mergesMultiError(t *testing.T) {
	err := chunkedMulti(5, 2, func(start, end int) error {
		merr := make(datastore.MultiError, end-start)
		if start == 2 {
			merr[0] = datastore.ErrNoSuchEntity
		}
		if start == 4 {
			merr[0] = errors.New("boom")
		}
		for _, e := range merr {
			if e != nil {
				return merr
			}
		}
		return nil
	})
	var merr datastore.MultiError
	require.ErrorAs(t, err, &merr)
	require.Len(t, merr, 5)
	require.Nil(t, merr[0])
	require.Nil(t, merr[1])
	require.ErrorIs(t, merr[2], datastore.ErrNoSuchEntity)
	require.Nil(t, merr[3])
	require.EqualError(t, merr[4], "boom")
}

func TestChunkedMulti_returnsNonMultiErrorImmediately(t *testing.T) {
	calls := 0
	err := chunkedMulti(5, 2, func(start, end int) error {
		calls++
		if start == 2 {
			return errors.New("hard failure")
		}
		return nil
	})
	require.EqualError(t, err, "hard failure")
	require.Equal(t, 2, calls)
}

func TestGetMulti_lengthMismatch(t *testing.T) {
	err := GetMulti(t.Context(), make([]*datastore.Key, 2), make([]any, 1))
	require.EqualError(t, err, "entitystore: key and dst slices have different length")
}

func TestPutMulti_nonSliceSrc(t *testing.T) {
	keys := make([]*datastore.Key, DatastoreMultiLimit+1)
	err := PutMulti(t.Context(), keys, "not-a-slice")
	require.EqualError(t, err, "entitystore: src has type string; want a slice")
}

func TestPutMulti_lengthMismatch(t *testing.T) {
	keys := make([]*datastore.Key, DatastoreMultiLimit+1)
	err := PutMulti(t.Context(), keys, make([]string, DatastoreMultiLimit))
	require.EqualError(t, err, "entitystore: key and src slices have different length")
}

func TestPutMulti_empty(t *testing.T) {
	err := PutMulti(t.Context(), nil, []string{})
	require.NoError(t, err)
}
