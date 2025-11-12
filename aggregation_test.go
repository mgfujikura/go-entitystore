package entitystore

import (
	"context"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCount(t *testing.T) {
	ctx := context.Background()
	DefaultTestInitialize(ctx, nil)

	err := PutEntityMulti(ctx, []*AggregationTestEntity{
		{Id: 1, Value: 10},
		{Id: 2, Value: 20},
		{Id: 3, Value: 30},
	})
	require.NoError(t, err)

	q := datastore.NewQuery("AggregationTestEntity").FilterField("Value", ">=", 15)
	count, err := Count(ctx, *q)
	require.NoError(t, err)
	require.Equal(t, 2, count)
}

func TestAvg(t *testing.T) {
	ctx := context.Background()
	DefaultTestInitialize(ctx, nil)

	err := PutEntityMulti(ctx, []*AggregationTestEntity{
		{Id: 1, Value: 10},
		{Id: 2, Value: 20},
		{Id: 3, Value: 30},
	})
	require.NoError(t, err)

	q := datastore.NewQuery("AggregationTestEntity").FilterField("Value", ">=", 15)
	avg, err := Avg(ctx, *q, "Value")
	require.NoError(t, err)
	require.Equal(t, 25.0, avg)
}

func TestIntSum(t *testing.T) {
	ctx := context.Background()
	DefaultTestInitialize(ctx, nil)

	err := PutEntityMulti(ctx, []*AggregationTestEntity{
		{Id: 1, Value: 10, Value2: 1.5},
		{Id: 2, Value: 20, Value2: 2.5},
		{Id: 3, Value: 30, Value2: 3.5},
	})
	require.NoError(t, err)

	q := datastore.NewQuery("AggregationTestEntity").FilterField("Value", ">=", 15)
	sum, err := IntSum(ctx, *q, "Value")
	require.NoError(t, err)
	require.Equal(t, 50, sum)
}

func TestFloat64Sum(t *testing.T) {
	ctx := context.Background()
	DefaultTestInitialize(ctx, nil)

	err := PutEntityMulti(ctx, []*AggregationTestEntity{
		{Id: 1, Value: 10, Value2: 1.5},
		{Id: 2, Value: 20, Value2: 2.5},
		{Id: 3, Value: 30, Value2: 3.5},
	})
	require.NoError(t, err)

	q := datastore.NewQuery("AggregationTestEntity").FilterField("Value2", ">=", 2.0)
	sum, err := Float64Sum(ctx, *q, "Value2")
	require.NoError(t, err)
	require.Equal(t, 6.0, sum)
}

func TestAggregation(t *testing.T) {
	ctx := context.Background()
	DefaultTestInitialize(ctx, nil)

	err := PutEntityMulti(ctx, []*AggregationTestEntity{
		{Id: 1, Value: 10, Value2: 1.5},
		{Id: 2, Value: 20, Value2: 2.5},
		{Id: 3, Value: 30, Value2: 3.5},
	})
	require.NoError(t, err)

	q := datastore.NewQuery("AggregationTestEntity")
	a := NewAggregation(q).WithCount().WithAvg("Value").WithIntSum("Value").WithFloat64Sum("Value2")
	err = a.Run(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, a.Count())
	assert.Equal(t, 20.0, a.Avg("Value"))
	assert.Equal(t, 60, a.IntSum("Value"))
	assert.Equal(t, 7.5, a.Float64Sum("Value2"))
}
