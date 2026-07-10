package entitystore

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/stretchr/testify/require"

	"go.fujikura.biz/entitystore/cachestore"
)

type failDeleteCache struct {
	cachestore.Nostore
}

func (failDeleteCache) DeleteEntities(context.Context, []datastore.Key) error {
	return errors.New("cache delete failed")
}

func TestTransaction_invalidateCache(t *testing.T) {
	ctx := context.Background()
	cs := &cachestore.Memorystore{}
	key := *datastore.NameKey("TestEntity", "1", nil)
	require.NoError(t, cs.SetEntities(ctx, map[datastore.Key][]datastore.Property{
		key: {{Name: "Value", Value: "cached"}},
	}))

	prevCache, prevLogger := cache, logger
	t.Cleanup(func() {
		cache, logger = prevCache, prevLogger
	})
	cache = cs
	logger = slog.Default()

	tx := &transaction{}
	tx.TrackKey(&key)
	require.NoError(t, tx.invalidateCache(ctx, nil))
	require.Len(t, cs.Cache, 0)
}

func TestTransaction_invalidateCache_dedupesKeys(t *testing.T) {
	ctx := context.Background()
	cs := &cachestore.Memorystore{}
	key := *datastore.NameKey("TestEntity", "1", nil)
	require.NoError(t, cs.SetEntities(ctx, map[datastore.Key][]datastore.Property{
		key: {{Name: "Value", Value: "cached"}},
	}))

	prevCache, prevLogger := cache, logger
	t.Cleanup(func() {
		cache, logger = prevCache, prevLogger
	})
	cache = cs
	logger = slog.Default()

	tx := &transaction{}
	tx.TrackKey(&key)
	tx.TrackKey(&key)
	require.NoError(t, tx.invalidateCache(ctx, nil))
	require.Len(t, cs.Cache, 0)
}

func TestTransaction_invalidateCache_wrapsError(t *testing.T) {
	ctx := context.Background()
	prevCache, prevLogger := cache, logger
	t.Cleanup(func() {
		cache, logger = prevCache, prevLogger
	})
	cache = failDeleteCache{}
	logger = slog.Default()

	key := *datastore.NameKey("TestEntity", "1", nil)
	tx := &transaction{}
	tx.TrackKey(&key)
	err := tx.invalidateCache(ctx, nil)
	require.ErrorIs(t, err, ErrCacheInvalidate)
}

func TestTransaction_trackPut_completeKey(t *testing.T) {
	key := datastore.NameKey("TestEntity", "1", nil)
	tx := &transaction{}
	tx.trackPut(key, nil)
	require.Len(t, tx.keys, 1)
	require.Equal(t, *key, tx.keys[0])
	require.Empty(t, tx.pendingKeys)
}

func TestTransaction_trackPut_incompleteKey(t *testing.T) {
	key := datastore.IncompleteKey("TestEntity", nil)
	tx := &transaction{}
	tx.trackPut(key, nil)
	require.Empty(t, tx.keys)
	require.Len(t, tx.pendingKeys, 1)
}

func TestRunInTransaction_GetEntityTx_ignoresCache(t *testing.T) {
	ctx := context.Background()
	cs := &cachestore.Memorystore{}
	DefaultTestInitialize(t, ctx, cs)

	stored := TestEntity{Id: 1, Value: "from-datastore"}
	require.NoError(t, PutEntity(ctx, &stored))

	stale := TestEntity{Id: 1, Value: "from-cache"}
	ps, err := datastore.SaveStruct(&stale)
	require.NoError(t, err)
	require.NoError(t, cs.SetEntities(ctx, map[datastore.Key][]datastore.Property{
		*stored.Key(): ps,
	}))

	_, err = RunInTransaction(ctx, func(tx Transaction) error {
		e := TestEntity{Id: 1}
		if err := GetEntityTx(tx, &e); err != nil {
			return err
		}
		require.Equal(t, "from-datastore", e.Value)
		return nil
	})
	require.NoError(t, err)
}

func TestRunInTransaction_PutEntityTx_commitsAndInvalidatesCache(t *testing.T) {
	ctx := context.Background()
	cs := &cachestore.Memorystore{}
	DefaultTestInitialize(t, ctx, cs)
	Now = func() time.Time {
		return time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	t.Cleanup(func() { Now = time.Now })

	stored := TestEntity{Id: 1, Value: "old"}
	require.NoError(t, PutEntity(ctx, &stored))

	ps, err := datastore.SaveStruct(&TestEntity{Id: 1, Value: "stale"})
	require.NoError(t, err)
	require.NoError(t, cs.SetEntities(ctx, map[datastore.Key][]datastore.Property{
		*stored.Key(): ps,
	}))
	require.Len(t, cs.Cache, 1)

	_, err = RunInTransaction(ctx, func(tx Transaction) error {
		e := TestEntity{Id: 1, Value: "new"}
		return PutEntityTx(ctx, tx, &e)
	})
	require.NoError(t, err)
	require.Len(t, cs.Cache, 0)

	got := TestEntity{Id: 1}
	require.NoError(t, GetEntity(ctx, &got))
	require.Equal(t, "new", got.Value)
	require.Equal(t, time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC), got.UpdatedAt())
}

func TestRunInTransaction_rollback_doesNotCommitOrInvalidateCache(t *testing.T) {
	ctx := context.Background()
	cs := &cachestore.Memorystore{}
	DefaultTestInitialize(t, ctx, cs)

	stored := TestEntity{Id: 1, Value: "old"}
	require.NoError(t, PutEntity(ctx, &stored))

	ps, err := datastore.SaveStruct(&stored)
	require.NoError(t, err)
	require.NoError(t, cs.SetEntities(ctx, map[datastore.Key][]datastore.Property{
		*stored.Key(): ps,
	}))
	require.Len(t, cs.Cache, 1)

	_, err = RunInTransaction(ctx, func(tx Transaction) error {
		e := TestEntity{Id: 1, Value: "should-not-commit"}
		if err := PutEntityTx(ctx, tx, &e); err != nil {
			return err
		}
		return errors.New("force rollback")
	})
	require.Error(t, err)
	require.Len(t, cs.Cache, 1)

	got := TestEntity{Id: 1}
	require.NoError(t, GetEntity(ctx, &got))
	require.Equal(t, "old", got.Value)
}

func TestRunInTransaction_DeleteEntityTx(t *testing.T) {
	ctx := context.Background()
	cs := &cachestore.Memorystore{}
	DefaultTestInitialize(t, ctx, cs)

	stored := TestEntity{Id: 1, Value: "to-delete"}
	require.NoError(t, PutEntity(ctx, &stored))
	ps, err := datastore.SaveStruct(&stored)
	require.NoError(t, err)
	require.NoError(t, cs.SetEntities(ctx, map[datastore.Key][]datastore.Property{
		*stored.Key(): ps,
	}))

	_, err = RunInTransaction(ctx, func(tx Transaction) error {
		return DeleteEntityTx(tx, &TestEntity{Id: 1})
	})
	require.NoError(t, err)
	require.Len(t, cs.Cache, 0)

	got := TestEntity{Id: 1}
	err = GetEntity(ctx, &got)
	require.ErrorIs(t, err, datastore.ErrNoSuchEntity)
}

func TestRunInTransaction_PutEntityMultiTx_and_GetEntityMultiTx(t *testing.T) {
	ctx := context.Background()
	cs := &cachestore.Memorystore{}
	DefaultTestInitialize(t, ctx, cs)

	require.NoError(t, PutEntityMulti(ctx, []*TestEntity{
		{Id: 1, Value: "a"},
		{Id: 2, Value: "b"},
	}))

	_, err := RunInTransaction(ctx, func(tx Transaction) error {
		es := []*TestEntity{{Id: 1}, {Id: 2}}
		if err := GetEntityMultiTx(tx, es); err != nil {
			return err
		}
		require.Equal(t, "a", es[0].Value)
		require.Equal(t, "b", es[1].Value)
		es[0].Value = "a2"
		es[1].Value = "b2"
		return PutEntityMultiTx(ctx, tx, es)
	})
	require.NoError(t, err)

	es := []*TestEntity{{Id: 1}, {Id: 2}}
	require.NoError(t, GetEntityMulti(ctx, es))
	require.Equal(t, "a2", es[0].Value)
	require.Equal(t, "b2", es[1].Value)
}

func TestRunInTransaction_DeleteEntityMultiTx(t *testing.T) {
	ctx := context.Background()
	cs := &cachestore.Memorystore{}
	DefaultTestInitialize(t, ctx, cs)

	require.NoError(t, PutEntityMulti(ctx, []*TestEntity{
		{Id: 1, Value: "a"},
		{Id: 2, Value: "b"},
	}))

	_, err := RunInTransaction(ctx, func(tx Transaction) error {
		return DeleteEntityMultiTx(tx, []*TestEntity{{Id: 1}, {Id: 2}})
	})
	require.NoError(t, err)

	es := []*TestEntity{{Id: 1}, {Id: 2}}
	err = GetEntityMulti(ctx, es)
	require.Error(t, err)
	var merr datastore.MultiError
	require.ErrorAs(t, err, &merr)
	require.ErrorIs(t, merr[0], datastore.ErrNoSuchEntity)
	require.ErrorIs(t, merr[1], datastore.ErrNoSuchEntity)
}

func TestRunInTransaction_MutateEntityTx(t *testing.T) {
	ctx := context.Background()
	cs := &cachestore.Memorystore{}
	DefaultTestInitialize(t, ctx, cs)

	require.NoError(t, PutEntityMulti(ctx, []*TestEntity{
		{Id: 1, Value: "old"},
		{Id: 2, Value: "to-delete"},
	}))
	ps1, err := datastore.SaveStruct(&TestEntity{Id: 1, Value: "stale1"})
	require.NoError(t, err)
	ps2, err := datastore.SaveStruct(&TestEntity{Id: 2, Value: "stale2"})
	require.NoError(t, err)
	require.NoError(t, cs.SetEntities(ctx, map[datastore.Key][]datastore.Property{
		*(&TestEntity{Id: 1}).Key(): ps1,
		*(&TestEntity{Id: 2}).Key(): ps2,
	}))
	require.Len(t, cs.Cache, 2)

	_, err = RunInTransaction(ctx, func(tx Transaction) error {
		return MutateEntityTx(ctx, tx,
			NewUpdate(&TestEntity{Id: 1, Value: "updated"}),
			NewDelete(&TestEntity{Id: 2}),
			NewInsert(&TestEntity{Id: 3, Value: "inserted"}),
			NewUpsert(&TestEntity{Id: 4, Value: "upserted"}),
		)
	})
	require.NoError(t, err)
	require.Len(t, cs.Cache, 0)

	es := []*TestEntity{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}}
	err = GetEntityMulti(ctx, es)
	require.Error(t, err)
	var merr datastore.MultiError
	require.ErrorAs(t, err, &merr)
	require.Nil(t, merr[0])
	require.ErrorIs(t, merr[1], datastore.ErrNoSuchEntity)
	require.Nil(t, merr[2])
	require.Nil(t, merr[3])
	require.Equal(t, "updated", es[0].Value)
	require.Equal(t, "inserted", es[2].Value)
	require.Equal(t, "upserted", es[3].Value)
}
