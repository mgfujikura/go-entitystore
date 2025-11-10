package entitystore

import (
	"context"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"go.fujikura.biz/entitystore/cachestore"
)

type NewClientCheck struct {
	Value string
}

type TestCachestore struct {
	cachestore.Nostore
}

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
