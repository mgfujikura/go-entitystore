package entitystore

import (
	"context"
	"strconv"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/option"

	"go.fujikura.biz/entitystore/cachestore"
)

type NewClientCheck struct {
	Value string
}

type TestCachestore struct {
	cachestore.Nostore
}

type TestEntity struct {
	EntityBase
	Id    int
	Value string
}

func (e *TestEntity) Key() *datastore.Key {
	return datastore.NameKey("TestEntity", strconv.Itoa(e.Id), nil)
}

func DefaultTestInitialize(ctx context.Context, cs cachestore.Cachestore) {
	Initialize(ctx, "entitystore-test-project", Config{
		Options: []option.ClientOption{
			option.WithCredentialsFile("service-account-key.json"),
		},
		Cachestore: cs,
	})
	DeleteAll(ctx, "TestEntity")
}
