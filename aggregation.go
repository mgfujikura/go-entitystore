package entitystore

import (
	"context"
	"errors"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
)

// Count はクエリに一致するエンティティの数を返します。
func Count(ctx context.Context, q Query) (int, error) {
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
func Avg(ctx context.Context, q Query, f string) (float64, error) {
	aq := q.NewAggregationQuery().WithAvg(f, "avg")
	ar, err := client.RunAggregationQuery(ctx, aq)
	if err != nil {
		return 0, err
	}
	if c, ok := ar["avg"]; ok {
		cv := c.(*datastorepb.Value)
		return cv.GetDoubleValue(), nil
	}
	return 0, errors.New("no avg")
}

// IntSum はクエリに一致するエンティティのInt型の指定フィールドの合計値を返します。
func IntSum(ctx context.Context, q Query, f string) (int, error) {
	aq := q.NewAggregationQuery().WithSum(f, "sum")
	ar, err := client.RunAggregationQuery(ctx, aq)
	if err != nil {
		return 0, err
	}
	if c, ok := ar["sum"]; ok {
		cv := c.(*datastorepb.Value)
		return int(cv.GetIntegerValue()), nil
	}
	return 0, errors.New("no sum")
}

// Float64Sum はクエリに一致するエンティティのFloat64型の指定フィールドの合計値を返します。
func Float64Sum(ctx context.Context, q Query, f string) (float64, error) {
	aq := q.NewAggregationQuery().WithSum(f, "sum")
	ar, err := client.RunAggregationQuery(ctx, aq)
	if err != nil {
		return 0, err
	}
	if c, ok := ar["sum"]; ok {
		cv := c.(*datastorepb.Value)
		return cv.GetDoubleValue(), nil
	}
	return 0, errors.New("no sum")
}

// Aggregation は複数の集計を一度に実行するためのインターフェースです。
type Aggregation interface {
	WithCount() Aggregation
	WithAvg(f string) Aggregation
	WithIntSum(f string) Aggregation
	WithFloat64Sum(f string) Aggregation
	Run(ctx context.Context) error
	Count() int
	Avg(f string) float64
	IntSum(f string) int
	Float64Sum(f string) float64
}

type aggregation struct {
	aq        *datastore.AggregationQuery
	iResults  map[string]int
	fResults  map[string]float64
	intKeys   []string
	floatKeys []string
}

// NewAggregation コンストラクタ
func NewAggregation(q Query) Aggregation {
	return &aggregation{
		aq:       q.NewAggregationQuery(),
		iResults: make(map[string]int),
		fResults: make(map[string]float64),
	}
}

// WithCount はカウント集計を追加します。
func (a *aggregation) WithCount() Aggregation {
	alias := "count"
	a.aq = a.aq.WithCount(alias)
	a.intKeys = append(a.intKeys, alias)
	return a
}

// WithAvg は指定フィールドの平均値集計を追加します。
func (a *aggregation) WithAvg(f string) Aggregation {
	alias := "avg_" + f
	a.aq = a.aq.WithAvg(f, alias)
	a.floatKeys = append(a.floatKeys, alias)
	return a
}

// WithIntSum は指定フィールドのInt型の合計値集計を追加します。
func (a *aggregation) WithIntSum(f string) Aggregation {
	alias := "isum_" + f
	a.aq = a.aq.WithSum(f, alias)
	a.intKeys = append(a.intKeys, alias)
	return a
}

// WithFloat64Sum は指定フィールドのFloat64型の合計値集計を追加します。
func (a *aggregation) WithFloat64Sum(f string) Aggregation {
	alias := "fsum_" + f
	a.aq = a.aq.WithSum(f, alias)
	a.floatKeys = append(a.floatKeys, alias)
	return a
}

// Run は集計クエリを実行します。
// 結果はAggregation構造体に保存され、Count、Avg、IntSum、Float64Sumメソッドで取得できます。
func (a *aggregation) Run(ctx context.Context) error {
	ar, err := client.RunAggregationQuery(ctx, a.aq)
	if err != nil {
		return err
	}
	for _, k := range a.intKeys {
		if v, ok := ar[k]; ok {
			a.iResults[k] = int(v.(*datastorepb.Value).GetIntegerValue())
		}
	}
	for _, k := range a.floatKeys {
		if v, ok := ar[k]; ok {
			a.fResults[k] = v.(*datastorepb.Value).GetDoubleValue()
		}
	}
	return nil
}

// Count はカウント集計の結果を返します。
func (a *aggregation) Count() int {
	return a.iResults["count"]
}

// Avg は指定フィールドの平均値集計の結果を返します。
func (a *aggregation) Avg(f string) float64 {
	return a.fResults["avg_"+f]
}

// IntSum は指定フィールドのInt型の合計値集計の結果を返します。
func (a *aggregation) IntSum(f string) int {
	return a.iResults["isum_"+f]
}

// Float64Sum は指定フィールドのFloat64型の合計値集計の結果を返します。
func (a *aggregation) Float64Sum(f string) float64 {
	return a.fResults["fsum_"+f]
}
