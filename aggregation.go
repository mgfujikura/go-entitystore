package entitystore

import (
	"context"
	"errors"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
)

// Count はクエリに一致するエンティティの数を返します。
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

// Avg はクエリに一致するエンティティの指定フィールドの平均値を返します。
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

// IntSum はクエリに一致するエンティティのInt型の指定フィールドの合計値を返します。
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

// Float64Sum はクエリに一致するエンティティのFloat64型の指定フィールドの合計値を返します。
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

// Aggregation は複数の集計を一度に実行するための構造体です。
type Aggregation struct {
	aq      *datastore.AggregationQuery
	iresuts map[string]int
	fresuts map[string]float64
}

// NewAggregation コンストラクタ
func NewAggregation(q *datastore.Query) *Aggregation {
	return &Aggregation{
		aq:      q.NewAggregationQuery(),
		iresuts: make(map[string]int),
		fresuts: make(map[string]float64),
	}
}

// WithCount はカウント集計を追加します。
func (a *Aggregation) WithCount() *Aggregation {
	a.aq = a.aq.WithCount("count")
	return a
}

// WithAvg は指定フィールドの平均値集計を追加します。
func (a *Aggregation) WithAvg(f string) *Aggregation {
	a.aq = a.aq.WithAvg(f, "avg_"+f)
	return a
}

// WithIntSum は指定フィールドのInt型の合計値集計を追加します。
func (a *Aggregation) WithIntSum(f string) *Aggregation {
	a.aq = a.aq.WithSum(f, "isum_"+f)
	return a
}

// WithFloat64Sum は指定フィールドのFloat64型の合計値集計を追加します。
func (a *Aggregation) WithFloat64Sum(f string) *Aggregation {
	a.aq = a.aq.WithSum(f, "fsum_"+f)
	return a
}

// Run は集計クエリを実行します。
// 結果はAggregation構造体に保存され、Count、Avg、IntSum、Float64Sumメソッドで取得できます。
func (a *Aggregation) Run(ctx context.Context) error {
	ar, err := client.RunAggregationQuery(ctx, a.aq)
	if err != nil {
		return err
	}
	for k, v := range ar {
		cv := v.(*datastorepb.Value)
		if k[:5] == "isum_" || k == "count" {
			a.iresuts[k] = int(cv.GetIntegerValue())
		} else if k[:5] == "fsum_" || k[:4] == "avg_" {
			a.fresuts[k] = cv.GetDoubleValue()
		}
	}
	return nil
}

// Count はカウント集計の結果を返します。
func (a *Aggregation) Count() int {
	return a.iresuts["count"]
}

// Avg は指定フィールドの平均値集計の結果を返します。
func (a *Aggregation) Avg(f string) float64 {
	return a.fresuts["avg_"+f]
}

// IntSum は指定フィールドのInt型の合計値集計の結果を返します。
func (a *Aggregation) IntSum(f string) int {
	return a.iresuts["isum_"+f]
}

// Float64Sum は指定フィールドのFloat64型の合計値集計の結果を返します。
func (a *Aggregation) Float64Sum(f string) float64 {
	return a.fresuts["fsum_"+f]
}
