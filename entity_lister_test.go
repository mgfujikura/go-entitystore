package entitystore

import (
	"context"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEntityLister_GetList(t *testing.T) {
	ctx := context.Background()
	DefaultTestInitialize(ctx, nil)

	err := PutEntityMulti(ctx, []*TestEntity{
		{Id: 1, Value: "Test1"},
		{Id: 2, Value: "Test2"},
		{Id: 3, Value: "Test3"},
	})
	require.NoError(t, err)

	q := datastore.NewQuery("TestEntity").Order("-Id")
	lister := NewEntityLister(q, &TestEntity{})
	es, cur, err := lister.GetList(ctx, 2, "")

	require.NoError(t, err)
	require.Len(t, es, 2)
	assert.Equal(t, 3, es[0].Id)
	assert.Equal(t, 2, es[1].Id)
	require.NotEmpty(t, cur)

	es2, cur2, err := lister.GetList(ctx, 2, cur)
	require.NoError(t, err)
	require.Len(t, es2, 1)
	assert.Equal(t, 1, es2[0].Id)
	require.Empty(t, cur2)
}

func TestEntityLister_WithFilter(t *testing.T) {
	ctx := context.Background()
	DefaultTestInitialize(ctx, nil)

	err := PutEntityMulti(ctx, []*TestEntity{
		{Id: 1, Value: "Test1"},
		{Id: 2, Value: "Test2"},
		{Id: 3, Value: "Test3"},
	})
	require.NoError(t, err)

	q := datastore.NewQuery("TestEntity").Order("-Id")
	lister := NewEntityLister(q, &TestEntity{}).WithFilter(func(key *datastore.Key) bool {
		return key.Name != "3"
	})
	es, cur, err := lister.GetList(ctx, 2, "")

	require.NoError(t, err)
	require.Len(t, es, 2)
	assert.Equal(t, 2, es[0].Id)
	assert.Equal(t, 1, es[1].Id)
	require.Empty(t, cur)
}

func TestEntityLister_GetKeyList(t *testing.T) {
	ctx := context.Background()
	DefaultTestInitialize(ctx, nil)

	err := PutEntityMulti(ctx, []*TestEntity{
		{Id: 1, Value: "Test1"},
		{Id: 2, Value: "Test2"},
		{Id: 3, Value: "Test3"},
	})
	require.NoError(t, err)

	q := datastore.NewQuery("TestEntity").Order("-Id")
	lister := NewEntityLister(q, &TestEntity{}).WithFilter(func(key *datastore.Key) bool {
		return key.Name != "3"
	})
	keys, cur, err := lister.GetKeyList(ctx, 2, "")

	require.NoError(t, err)
	require.Len(t, keys, 2)
	assert.Equal(t, "2", keys[0].Name)
	assert.Equal(t, "1", keys[1].Name)
	require.Empty(t, cur)
}
