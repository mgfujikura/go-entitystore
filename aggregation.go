package entitystore

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
)

func Count(ctx context.Context, q datastore.Query) (int, error) {
	aq := q.NewAggregationQuery().WithCount("count")
	ar, err := client.RunAggregationQuery(ctx, aq)
	if err != nil {
		return 0, err
	}
	if c, ok := ar["count"]; ok {
		cv := c.(*datastorepb.Value)
		return int(cv.GetIntegerValue()), nil
	}
	return 0, errors.New("no count")
}

func Avg(ctx context.Context, q datastore.Query, f string) (float64, error) {
	aq := q.NewAggregationQuery().WithAvg(f, "avg")
	ar, err := client.RunAggregationQuery(ctx, aq)
	if err != nil {
		return 0, err
	}
	if c, ok := ar["avg"]; ok {
		cv := c.(*datastorepb.Value)
		return cv.GetDoubleValue(), nil
	}
	return 0, errors.New("no count")
}

func IntSum(ctx context.Context, q datastore.Query, f string) (int, error) {
	aq := q.NewAggregationQuery().WithSum(f, "sum")
	ar, err := client.RunAggregationQuery(ctx, aq)
	if err != nil {
		return 0, err
	}
	if c, ok := ar["sum"]; ok {
		cv := c.(*datastorepb.Value)
		return int(cv.GetIntegerValue()), nil
	}
	return 0, errors.New("no count")
}

func Float64Sum(ctx context.Context, q datastore.Query, f string) (float64, error) {
	aq := q.NewAggregationQuery().WithSum(f, "sum")
	ar, err := client.RunAggregationQuery(ctx, aq)
	if err != nil {
		return 0, err
	}
	if c, ok := ar["sum"]; ok {
		cv := c.(*datastorepb.Value)
		return cv.GetDoubleValue(), nil
	}
	return 0, errors.New("no count")
}

type Aggregation struct {
	aq      *datastore.AggregationQuery
	iresuts map[string]int
	fresuts map[string]float64
}

func NewAggregation(q *datastore.Query) *Aggregation {
	return &Aggregation{
		aq:      q.NewAggregationQuery(),
		iresuts: make(map[string]int),
		fresuts: make(map[string]float64),
	}
}

func (a *Aggregation) WithCount() *Aggregation {
	a.aq = a.aq.WithCount("count")
	return a
}
func (a *Aggregation) WithAvg(f string) *Aggregation {
	a.aq = a.aq.WithAvg(f, "avg_"+f)
	return a
}
func (a *Aggregation) WithIntSum(f string) *Aggregation {
	a.aq = a.aq.WithSum(f, "isum_"+f)
	return a
}
func (a *Aggregation) WithFloat64Sum(f string) *Aggregation {
	a.aq = a.aq.WithSum(f, "fsum_"+f)
	return a
}
func (a *Aggregation) Run(ctx context.Context) error {
	ar, err := client.RunAggregationQuery(ctx, a.aq)
	if err != nil {
		return err
	}
	fmt.Printf("ar: %#v\n", ar)
	for k, v := range ar {
		println(k)
		cv := v.(*datastorepb.Value)
		if k[:5] == "isum_" || k == "count" {
			a.iresuts[k] = int(cv.GetIntegerValue())
		} else if k[:5] == "fsum_" || k[:4] == "avg_" {
			a.fresuts[k] = cv.GetDoubleValue()
		}
	}
	return nil
}

func (a *Aggregation) Count() int {
	return a.iresuts["count"]
}
func (a *Aggregation) Avg(f string) float64 {
	return a.fresuts["avg_"+f]
}
func (a *Aggregation) IntSum(f string) int {
	return a.iresuts["isum_"+f]
}
func (a *Aggregation) Float64Sum(f string) float64 {
	return a.fresuts["fsum_"+f]
}
