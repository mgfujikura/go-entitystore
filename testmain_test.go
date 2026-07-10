package entitystore

import (
	"context"
	"strconv"
	"testing"

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

type AggregationTestEntity struct {
	EntityBase
	Id     int
	Value  int
	Value2 float64
}

func (e *AggregationTestEntity) Key() *datastore.Key {
	return datastore.NameKey("AggregationTestEntity", strconv.Itoa(e.Id), nil)
}

// requireIntegration は実 GCP Datastore を使う結合テスト用です。
// go test -short ではスキップされます。
func requireIntegration(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Datastore integration test; run without -short against a real GCP project")
	}
}

func DefaultTestInitialize(t *testing.T, ctx context.Context, cs cachestore.Cachestore) {
	t.Helper()
	requireIntegration(t)
	Initialize(ctx, "entitystore-test-project", Config{
		Options: []option.ClientOption{
			option.WithCredentialsFile("service-account-key.json"),
		},
		Cachestore: cs,
	})
	err := DeleteAll(ctx, "TestEntity")
	if err != nil {
		panic(err)
	}
}
