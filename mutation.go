package entitystore

import "cloud.google.com/go/datastore"

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
	Entity []datastore.Property
}

func NewDelete[E Entity](e E) *Mutation {
	return &Mutation{MutationTypeDelete, e.Key(), nil}
}

func NewInsert[E Entity](e E) *Mutation {
	return &Mutation{MutationTypeDelete, e.Key(), EntityToProperties(e)}
}

func NewUpdate[E Entity](e E) *Mutation {
	return &Mutation{MutationTypeDelete, e.Key(), EntityToProperties(e)}
}

func NewUpsert[E Entity](e E) *Mutation {
	return &Mutation{MutationTypeDelete, e.Key(), EntityToProperties(e)}
}
