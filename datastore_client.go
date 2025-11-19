package entitystore

import (
	"context"

	"cloud.google.com/go/datastore"
)

type DatastoreClient interface {
	AllocateIDs(ctx context.Context, keys []*datastore.Key) ([]*datastore.Key, error)
	ReserveIDs(ctx context.Context, keys []*datastore.Key) error
	Close() error
	Get(ctx context.Context, key *datastore.Key, dst interface{}) (err error)
	GetMulti(ctx context.Context, keys []*datastore.Key, dst interface{}) (err error)
	Put(ctx context.Context, key *datastore.Key, src interface{}) (*datastore.Key, error)
	PutWithOptions(ctx context.Context, req *datastore.PutRequest, opts ...datastore.PutOption) (*datastore.Key, error)
	PutMulti(ctx context.Context, keys []*datastore.Key, src interface{}) (ret []*datastore.Key, err error)
	PutMultiWithOptions(ctx context.Context, reqs []*datastore.PutRequest, opts ...datastore.PutOption) (ret []*datastore.Key, err error)
	Delete(ctx context.Context, key *datastore.Key) error
	DeleteMulti(ctx context.Context, keys []*datastore.Key) (err error)
	Mutate(ctx context.Context, muts ...*datastore.Mutation) (ret []*datastore.Key, err error)
	WithReadOptions(ro ...datastore.ReadOption) *datastore.Client
	NewTransaction(ctx context.Context, opts ...datastore.TransactionOption) (t *datastore.Transaction, err error)
	RunInTransaction(ctx context.Context, f func(tx *datastore.Transaction) error, opts ...datastore.TransactionOption) (cmt *datastore.Commit, err error)
	Count(ctx context.Context, q *datastore.Query) (n int, err error)
	GetAll(ctx context.Context, q *datastore.Query, dst interface{}) (keys []*datastore.Key, err error)
	GetAllWithOptions(ctx context.Context, q *datastore.Query, dst interface{}, opts ...datastore.RunOption) (res datastore.GetAllWithOptionsResult, err error)
	Run(ctx context.Context, q *datastore.Query) (it *datastore.Iterator)
	RunWithOptions(ctx context.Context, q *datastore.Query, opts ...datastore.RunOption) (it *datastore.Iterator)
	RunAggregationQuery(ctx context.Context, aq *datastore.AggregationQuery) (ar datastore.AggregationResult, err error)
	RunAggregationQueryWithOptions(ctx context.Context, aq *datastore.AggregationQuery, opts ...datastore.RunOption) (ar datastore.AggregationWithOptionsResult, err error)
}
