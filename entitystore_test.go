package entitystore

import (
	"context"
	"errors"
	"log/slog"
	"os"
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

	// 他の設定(デフォルト)確認
	require.Equal(t, cache, cachestore.Nostore{})
	require.Equal(t, slog.Default(), logger)
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

func TestInitialize_ロガー指定(t *testing.T) {
	ctx := context.Background()
	testLogger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	Initialize(ctx, "entitystore-test-project", Config{
		Options: []option.ClientOption{
			option.WithCredentialsFile("service-account-key.json"),
		},
		Logger: testLogger,
	})

	require.Equal(t, testLogger, logger)
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
	cs := &cachestore.Memorystore{}
	DefaultTestInitialize(ctx, cs)

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

	require.Len(t, cs.Cache, 1)
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

func TestGetEntityMulti_datastoreとcacheから取得し取得出来なかったものもある(t *testing.T) {
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
		{
			Id: 999,
		},
	}
	err = GetEntityMulti(ctx, es)
	require.NotNil(t, err)
	var merr datastore.MultiError
	errors.As(err, &merr)
	require.Len(t, merr, 3)
	require.Nil(t, merr[0])
	require.Nil(t, merr[1])
	require.Equal(t, datastore.ErrNoSuchEntity, merr[2])

	require.Equal(t, stored1.Value, es[0].Value)
	require.Equal(t, stored2.Value, es[1].Value)
	require.Equal(t, "", es[2].Value)

	require.Len(t, cs.Cache, 2)
}

func TestPutEntity(t *testing.T) {
	ctx := context.Background()
	cs := &cachestore.Memorystore{}
	DefaultTestInitialize(ctx, cs)

	stored1 := TestEntity{
		Id:    1,
		Value: "Test Old Value",
	}
	ps1, err := datastore.SaveStruct(&stored1)
	require.Nil(t, err)
	stored2 := TestEntity{
		Id:    2,
		Value: "Test Old Value 2",
	}
	ps2, err := datastore.SaveStruct(&stored2)
	require.Nil(t, err)

	err = cs.SetEntities(ctx, map[datastore.Key][]datastore.Property{
		*stored1.Key(): ps1,
		*stored2.Key(): ps2,
	})
	require.Nil(t, err)
	require.Len(t, cs.Cache, 2)

	err = PutEntity(ctx, &TestEntity{
		Id:    1,
		Value: "Test Value",
	})
	require.Nil(t, err)
	require.Len(t, cs.Cache, 1)

	e := TestEntity{
		Id: 1,
	}
	err = GetEntity(ctx, &e)
	require.Nil(t, err)
	require.Equal(t, "Test Value", e.Value)
}

func TestPutEntityMulti(t *testing.T) {
	ctx := context.Background()
	cs := &cachestore.Memorystore{}
	DefaultTestInitialize(ctx, cs)

	stored1 := TestEntity{
		Id:    1,
		Value: "Test Old Value",
	}
	ps1, err := datastore.SaveStruct(&stored1)
	require.Nil(t, err)
	stored2 := TestEntity{
		Id:    2,
		Value: "Test Old Value 2",
	}
	ps2, err := datastore.SaveStruct(&stored2)
	require.Nil(t, err)

	err = cs.SetEntities(ctx, map[datastore.Key][]datastore.Property{
		*stored1.Key(): ps1,
		*stored2.Key(): ps2,
	})
	require.Nil(t, err)
	require.Len(t, cs.Cache, 2)

	err = PutEntityMulti(ctx, []*TestEntity{
		{
			Id:    1,
			Value: "Test Value",
		},
		{
			Id:    3,
			Value: "Test Value 3",
		},
	})
	require.Nil(t, err)
	require.Len(t, cs.Cache, 1)

	e := TestEntity{
		Id: 1,
	}
	err = GetEntity(ctx, &e)
	require.Nil(t, err)
	require.Equal(t, "Test Value", e.Value)
	e = TestEntity{
		Id: 3,
	}
	err = GetEntity(ctx, &e)
	require.Nil(t, err)
	require.Equal(t, "Test Value 3", e.Value)
}

func TestDeleteEntity(t *testing.T) {
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

	err = PutEntityMulti(ctx, []*TestEntity{&stored1, &stored2})
	require.Nil(t, err)

	err = cs.SetEntities(ctx, map[datastore.Key][]datastore.Property{
		*stored1.Key(): ps1,
		*stored2.Key(): ps2,
	})
	require.Nil(t, err)
	require.Len(t, cs.Cache, 2)

	err = DeleteEntity(ctx, &TestEntity{
		Id: 1,
	})
	require.Nil(t, err)
	require.Len(t, cs.Cache, 1)

	err = GetEntity(ctx, &TestEntity{
		Id: 1,
	})
	require.Equal(t, datastore.ErrNoSuchEntity, err)
}

func TestDeleteEntityMulti(t *testing.T) {
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

	err = PutEntityMulti(ctx, []*TestEntity{&stored1, &stored2})
	require.Nil(t, err)

	err = cs.SetEntities(ctx, map[datastore.Key][]datastore.Property{
		*stored1.Key(): ps1,
		*stored2.Key(): ps2,
	})
	require.Nil(t, err)
	require.Len(t, cs.Cache, 2)

	err = DeleteEntityMulti(ctx, []*TestEntity{
		{
			Id: 1,
		},
		{
			Id: 2,
		},
	})
	require.Nil(t, err)
	require.Len(t, cs.Cache, 0)

	err = GetEntity(ctx, &TestEntity{
		Id: 1,
	})
	require.Equal(t, datastore.ErrNoSuchEntity, err)
	err = GetEntity(ctx, &TestEntity{
		Id: 2,
	})
	require.Equal(t, datastore.ErrNoSuchEntity, err)
}

func TestGetEntityAll(t *testing.T) {
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

	err = PutEntityMulti(ctx, []*TestEntity{&stored1, &stored2})
	require.Nil(t, err)

	err = cs.SetEntities(ctx, map[datastore.Key][]datastore.Property{
		*stored1.Key(): ps1,
	})
	require.Nil(t, err)
	require.Len(t, cs.Cache, 1)

	q := datastore.NewQuery("TestEntity").Order("Id")
	var es []*TestEntity
	err = GetEntityAll(ctx, q, &es)
	require.Nil(t, err)
	require.Len(t, es, 2)
	require.Equal(t, stored1.Value, es[0].Value)
	require.Equal(t, stored2.Value, es[1].Value)

	require.Len(t, cs.Cache, 2)
}

func TestGetEntityFirst(t *testing.T) {
	ctx := context.Background()
	cs := &cachestore.Memorystore{}
	DefaultTestInitialize(ctx, cs)

	stored1 := TestEntity{
		Id:    1,
		Value: "Test Value",
	}
	stored2 := TestEntity{
		Id:    2,
		Value: "Test Value 2",
	}

	err := PutEntityMulti(ctx, []*TestEntity{&stored1, &stored2})
	require.Nil(t, err)

	q := datastore.NewQuery("TestEntity").Order("-Id")
	var e TestEntity
	err = GetEntityFirst(ctx, q, &e)
	require.Nil(t, err)
	require.Equal(t, stored2.Value, e.Value)

	require.Len(t, cs.Cache, 1)
}
