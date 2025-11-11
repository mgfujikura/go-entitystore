package entitystore

import (
	"context"
	"errors"

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

func Avg(ctx context.Context, q datastore.Query) (float64, error) {
	aq := q.NewAggregationQuery().WithAvg("Value", "avg")
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
