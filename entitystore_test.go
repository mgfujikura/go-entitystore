package entitystore

import (
	"context"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"go.fujikura.biz/entitystore/cachestore"
)

func TestInitialize_デフォルトデータベース(t *testing.T) {
	ctx := context.Background()
	Initialize(ctx, "entitystore-test-project", Config{
		Options: []option.ClientOption{
			option.WithCredentialsFile("service-account-key.json"),
		},
	})

	require.NotNil(t, client)

	// 事前にデフォルトデータベースに投入してあるデータを確認
	check := NewClientCheck{}
	err := client.Get(ctx, datastore.NameKey("NewClientCheck", "NewClientCheck", nil), &check)
	require.Nil(t, err)
	require.Equal(t, "Default Database", check.Value)
}

func TestInitialize_データベース指定(t *testing.T) {
	ctx := context.Background()
	Initialize(ctx, "entitystore-test-project", Config{
		DatabaseId: "test-database",
		Options: []option.ClientOption{
			option.WithCredentialsFile("service-account-key.json"),
		},
	})

	require.NotNil(t, client)

	// 事前に test-database に投入してあるデータを確認
	check := NewClientCheck{}
	err := client.Get(ctx, datastore.NameKey("NewClientCheck", "NewClientCheck", nil), &check)
	require.Nil(t, err)
	require.Equal(t, "Test Database", check.Value)
}

func TestInitialize_キャッシュストア指定(t *testing.T) {
	ctx := context.Background()
	Initialize(ctx, "entitystore-test-project", Config{
		Options: []option.ClientOption{
			option.WithCredentialsFile("service-account-key.json"),
		},
		Cachestore: TestCachestore{},
	})

	require.NotEqual(t, cache, cachestore.Nostore{})
	require.Equal(t, cache, TestCachestore{})
}

func TestDeleteAll(t *testing.T) {
	ctx := context.Background()
	Initialize(ctx, "entitystore-test-project", Config{
		Options: []option.ClientOption{
			option.WithCredentialsFile("service-account-key.json"),
		},
	})

	stored1 := TestEntity{
		Id:    1,
		Value: "Test Value",
	}
	stored2 := TestEntity{
		Id:    2,
		Value: "Test Value",
	}

	_, err := client.PutMulti(ctx,
		[]*datastore.Key{stored1.Key(), stored2.Key()},
		[]*TestEntity{&stored1, &stored2})
	require.Nil(t, err)

	err = DeleteAll(ctx, "TestEntity")
	require.Nil(t, err)

	q := datastore.NewQuery("TestEntity").KeysOnly()
	keys, err := client.GetAll(ctx, q, nil)
	require.Nil(t, err)
	require.Len(t, keys, 0)
}

func TestGetEntity_datastoreから取得(t *testing.T) {
	ctx := context.Background()
	DefaultTestInitialize(ctx, nil)

	stored := TestEntity{
		Id:    1,
		Value: "Test Value",
	}
	_, err := client.Put(ctx, stored.Key(), &stored)
	require.Nil(t, err)

	e := TestEntity{
		Id: 1,
	}
	err = GetEntity(ctx, &e)
	require.Nil(t, err)
	require.Equal(t, stored.Value, e.Value)
}

func TestGetEntity_キャッシュから取得(t *testing.T) {
	ctx := context.Background()
	cs := &cachestore.Memorystore{}
	DefaultTestInitialize(ctx, cs)

	stored := TestEntity{
		Id:    1,
		Value: "Test Value",
	}
	ps, err := datastore.SaveStruct(&stored)
	require.Nil(t, err)
	err = cs.SetEntities(ctx, map[datastore.Key][]datastore.Property{
		*stored.Key(): ps,
	})
	require.Nil(t, err)

	e := TestEntity{
		Id: 1,
	}
	err = GetEntity(ctx, &e)
	require.Nil(t, err)
	require.Equal(t, stored.Value, e.Value)
}

func TestGetEntity_存在しない(t *testing.T) {
	ctx := context.Background()
	DefaultTestInitialize(ctx, nil)

	e := TestEntity{
		Id: 999,
	}
	err := GetEntity(ctx, &e)
	require.Equal(t, datastore.ErrNoSuchEntity, err)
}

func TestGetEntityMulti_datastoreから取得(t *testing.T) {
	ctx := context.Background()
	cs := &cachestore.Memorystore{}
	DefaultTestInitialize(ctx, cs)
	require.Len(t, cs.Cache, 0)

	stored1 := TestEntity{
		Id:    1,
		Value: "Test Value",
	}
	stored2 := TestEntity{
		Id:    2,
		Value: "Test Value 2",
	}
	_, err := client.PutMulti(ctx,
		[]*datastore.Key{stored1.Key(), stored2.Key()},
		[]*TestEntity{&stored1, &stored2},
	)
	require.Nil(t, err)

	es := []*TestEntity{
		{
			Id: 1,
		},
		{
			Id: 2,
		},
	}
	err = GetEntityMulti(ctx, es)
	require.Nil(t, err)
	require.Equal(t, stored1.Value, es[0].Value)
	require.Equal(t, stored2.Value, es[1].Value)

	require.Len(t, cs.Cache, 2)
}

func TestGetEntityMulti_cacheから取得(t *testing.T) {
	ctx := context.Background()
	cs := &cachestore.Memorystore{}
	DefaultTestInitialize(ctx, cs)

	stored1 := TestEntity{
		Id:    1,
		Value: "Test Value",
	}
	ps1, err := datastore.SaveStruct(&stored1)
	require.Nil(t, err)
	stored2 := TestEntity{
		Id:    2,
		Value: "Test Value 2",
	}
	ps2, err := datastore.SaveStruct(&stored2)
	require.Nil(t, err)

	err = cs.SetEntities(ctx, map[datastore.Key][]datastore.Property{
		*stored1.Key(): ps1,
		*stored2.Key(): ps2,
	})
	require.Nil(t, err)

	es := []*TestEntity{
		{
			Id: 1,
		},
		{
			Id: 2,
		},
	}
	err = GetEntityMulti(ctx, es)
	require.Nil(t, err)
	require.Equal(t, stored1.Value, es[0].Value)
	require.Equal(t, stored2.Value, es[1].Value)
}

func TestGetEntityMulti_datastoreとcacheから取得(t *testing.T) {
	ctx := context.Background()
	cs := &cachestore.Memorystore{}
	DefaultTestInitialize(ctx, cs)

	stored1 := TestEntity{
		Id:    1,
		Value: "Test Value",
	}
	ps1, err := datastore.SaveStruct(&stored1)
	require.Nil(t, err)
	stored2 := TestEntity{
		Id:    2,
		Value: "Test Value 2",
	}

	err = cs.SetEntities(ctx, map[datastore.Key][]datastore.Property{
		*stored1.Key(): ps1,
	})
	require.Nil(t, err)
	_, err = client.Put(ctx, stored2.Key(), &stored2)

	es := []*TestEntity{
		{
			Id: 1,
		},
		{
			Id: 2,
		},
	}
	err = GetEntityMulti(ctx, es)
	require.Nil(t, err)
	require.Equal(t, stored1.Value, es[0].Value)
	require.Equal(t, stored2.Value, es[1].Value)

	require.Len(t, cs.Cache, 2)
}
