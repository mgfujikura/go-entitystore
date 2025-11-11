package entitystore

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/stretchr/testify/require"

	"go.fujikura.biz/entitystore/cachestore"
)

func TestMutateEntity(t *testing.T) {
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

	err = MutateEntity(ctx,
		NewUpdate(&TestEntity{Id: 1, Value: "Mutated Value"}),
		NewDelete(&TestEntity{Id: 2}),
		NewInsert(&TestEntity{Id: 3, Value: "Inserted Value"}),
		NewUpsert(&TestEntity{Id: 4, Value: "Upserted Value"}),
	)
	require.Nil(t, err)

	require.Len(t, cs.Cache, 0)

	es := []*TestEntity{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}}
	err = GetEntityMulti(ctx, es)
	require.NotNil(t, err)
	var merr datastore.MultiError
	errors.As(err, &merr)
	require.Len(t, merr, 4)
	require.Nil(t, merr[0])
	require.Equal(t, datastore.ErrNoSuchEntity, merr[1])
	require.Nil(t, merr[2])
	require.Nil(t, merr[3])
	require.Equal(t, "Mutated Value", es[0].Value)
	require.Equal(t, "Inserted Value", es[2].Value)
	require.Equal(t, "Upserted Value", es[3].Value)
}
