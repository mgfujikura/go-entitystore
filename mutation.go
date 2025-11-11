package entitystore

import (
	"context"

	"cloud.google.com/go/datastore"
	"github.com/samber/lo"
)

type MutationType = int

const (
	MutationTypeDelete MutationType = iota
	MutationTypeInsert
	MutationTypeUpdate
	MutationTypeUpsert
)

type Mutation struct {
	Type   MutationType
	Key    *datastore.Key
	Entity Entity
}

func NewDelete[E Entity](e E) *Mutation {
	return &Mutation{MutationTypeDelete, e.Key(), nil}
}

func NewInsert[E Entity](e E) *Mutation {
	return &Mutation{MutationTypeInsert, e.Key(), e}
}

func NewUpdate[E Entity](e E) *Mutation {
	return &Mutation{MutationTypeUpdate, e.Key(), e}
}

func NewUpsert[E Entity](e E) *Mutation {
	return &Mutation{MutationTypeUpsert, e.Key(), e}
}

func MutateEntity(ctx context.Context, muts ...*Mutation) error {
	_, err := client.Mutate(ctx, lo.Map(muts, func(m *Mutation, _ int) *datastore.Mutation {
		switch m.Type {
		case MutationTypeDelete:
			return datastore.NewDelete(m.Key)
		case MutationTypeInsert:
			return datastore.NewInsert(m.Key, m.Entity)
		case MutationTypeUpdate:
			return datastore.NewUpdate(m.Key, m.Entity)
		case MutationTypeUpsert:
			return datastore.NewUpsert(m.Key, m.Entity)
		default:
			panic("unknown mutation type")
		}
	})...)
	if err != nil {
		return err
	}
	return cache.DeleteEntities(ctx, lo.Map(muts, func(m *Mutation, _ int) datastore.Key {
		return *m.Key
	}))
}
