package cachestore

import (
	"context"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/stretchr/testify/require"
)

func TestMemorystore_GetEntities(t *testing.T) {
	ctx := context.Background()

	key1 := *datastore.NameKey("TestEntity", "value1", nil)
	key2 := *datastore.NameKey("TestEntity", "value2", nil)
	key3 := *datastore.NameKey("TestEntity", "value3", nil)
	key4 := *datastore.NameKey("TestEntity", "value4", nil)

	m := &Memorystore{}
	m.Cache = map[datastore.Key][]datastore.Property{
		key1: {
			{Name: "Value", Value: "cachedValue1"},
		},
		key2: {
			{Name: "Value", Value: "cachedValue2"},
		},
		key3: {
			{Name: "Value", Value: "cachedValue3"},
		},
	}

	cached, err := m.GetEntities(ctx, []datastore.Key{key1, key2, key4})
	require.Nil(t, err)
	require.NotNil(t, cached)
	require.Len(t, cached, 2)
	v, ok := cached[key1]
	require.True(t, ok)
	require.Equal(t, v, []datastore.Property{
		{Name: "Value", Value: "cachedValue1"},
	})
	v, ok = cached[key2]
	require.True(t, ok)
	require.Equal(t, v, []datastore.Property{
		{Name: "Value", Value: "cachedValue2"},
	})
}

func TestMemorystore_SetEntities(t *testing.T) {
	ctx := context.Background()
	key1 := *datastore.NameKey("TestEntity", "value1", nil)
	key2 := *datastore.NameKey("TestEntity", "value2", nil)

	m := &Memorystore{}
	err := m.SetEntities(ctx, map[datastore.Key][]datastore.Property{
		key1: {
			{Name: "Value", Value: "newCachedValue1"},
		},
		key2: {
			{Name: "Value", Value: "newCachedValue2"},
		},
	})
	require.Nil(t, err)
	require.NotNil(t, m.Cache)
	require.Len(t, m.Cache, 2)

	v, ok := m.Cache[key1]
	require.True(t, ok)
	require.Equal(t, v, []datastore.Property{
		{Name: "Value", Value: "newCachedValue1"},
	})
	v, ok = m.Cache[key2]
	require.True(t, ok)
	require.Equal(t, v, []datastore.Property{
		{Name: "Value", Value: "newCachedValue2"},
	})
}

func TestMemorystore_DeleteEntities(t *testing.T) {
	ctx := context.Background()
	key1 := *datastore.NameKey("TestEntity", "value1", nil)
	key2 := *datastore.NameKey("TestEntity", "value2", nil)
	key3 := *datastore.NameKey("TestEntity", "value3", nil)

	m := &Memorystore{}
	m.Cache = map[datastore.Key][]datastore.Property{
		key1: {
			{Name: "Value", Value: "cachedValue1"},
		},
		key2: {
			{Name: "Value", Value: "cachedValue2"},
		},
		key3: {
			{Name: "Value", Value: "cachedValue3"},
		},
	}

	err := m.DeleteEntities(ctx, []datastore.Key{key1, key2})
	require.Nil(t, err)
	require.Len(t, m.Cache, 1)
	_, ok := m.Cache[key3]
	require.True(t, ok)
}
