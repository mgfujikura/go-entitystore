package entitystore

import "cloud.google.com/go/datastore"

type Query interface {
	Ancestor(ancestor *datastore.Key) Query
	EventualConsistency() Query
	Namespace(ns string) Query
	Transaction(t *datastore.Transaction) Query
	FilterEntity(ef datastore.EntityFilter) Query
	Filter(filterStr string, value interface{}) Query
	FilterField(fieldName, operator string, value interface{}) Query
	Order(fieldName string) Query
	Project(fieldNames ...string) Query
	Distinct() Query
	DistinctOn(fieldNames ...string) Query
	KeysOnly() Query
	Limit(limit int) Query
	Offset(offset int) Query
	Start(c datastore.Cursor) Query
	End(c datastore.Cursor) Query
	NewAggregationQuery() *datastore.AggregationQuery

	Q() *datastore.Query
}

type query struct {
	*datastore.Query
	isKeysOnly bool
}

func NewQuery(kind string) Query {
	var q Query
	q = &query{
		datastore.NewQuery(kind),
		false,
	}
	return q
}

func (q query) Ancestor(ancestor *datastore.Key) Query {
	q.Query = q.Query.Ancestor(ancestor)
	return q
}

func (q query) EventualConsistency() Query {
	q.Query = q.Query.EventualConsistency()
	return q
}

func (q query) Namespace(ns string) Query {
	q.Query = q.Query.Namespace(ns)
	return q
}

func (q query) Transaction(t *datastore.Transaction) Query {
	q.Query = q.Query.Transaction(t)
	return q
}

func (q query) FilterEntity(ef datastore.EntityFilter) Query {
	q.Query = q.Query.FilterEntity(ef)
	return q
}

//goland:noinspection GoDeprecation
func (q query) Filter(filterStr string, value interface{}) Query {
	q.Query = q.Query.Filter(filterStr, value)
	return q
}

func (q query) FilterField(fieldName, operator string, value interface{}) Query {
	q.Query = q.Query.FilterField(fieldName, operator, value)
	return q
}

func (q query) Order(fieldName string) Query {
	q.Query = q.Query.Order(fieldName)
	return q
}

func (q query) Project(fieldNames ...string) Query {
	q.Query = q.Query.Project(fieldNames...)
	return q
}

func (q query) Distinct() Query {
	q.Query = q.Query.Distinct()
	return q
}

func (q query) DistinctOn(fieldNames ...string) Query {
	q.Query = q.Query.DistinctOn(fieldNames...)
	return q
}

func (q query) KeysOnly() Query {
	q.Query = q.Query.KeysOnly()
	q.isKeysOnly = true
	return q
}

func (q query) Limit(limit int) Query {
	q.Query = q.Query.Limit(limit)
	return q
}

func (q query) Offset(offset int) Query {
	q.Query = q.Query.Offset(offset)
	return q
}

func (q query) Start(c datastore.Cursor) Query {
	q.Query = q.Query.Start(c)
	return q
}

func (q query) End(c datastore.Cursor) Query {
	q.Query = q.Query.End(c)
	return q
}

func (q query) Q() *datastore.Query {
	return q.Query
}
