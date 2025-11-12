package entitystore

import (
	"context"
	"errors"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"
)

type EntityLister[E Entity] struct {
	e E
	q *datastore.Query
	f func(*datastore.Key) bool
}

func NewEntityLister[E Entity](q *datastore.Query, e E) *EntityLister[E] {
	return &EntityLister[E]{
		e: e,
		q: q,
	}
}

func (l *EntityLister[E]) WithFilter(f func(*datastore.Key) bool) *EntityLister[E] {
	l.f = f
	return l
}

func (l *EntityLister[E]) GetList(ctx context.Context, limit int, cur string) ([]E, string, error) {
	q := l.q.KeysOnly()
	if cur != "" {
		cursor, err := datastore.DecodeCursor(cur)
		if err != nil {
			return nil, "", err
		}
		q = q.Start(cursor)
	}
	itr := client.Run(ctx, q)
	var keys []*datastore.Key
	var ents []E
	constructor := entityConstructor(l.e)
	// キーの取得とエンティティの入れ物の準備
	for len(keys) < limit { // limit件数分取得
		key, err := itr.Next(nil)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, "", err
		}
		// フィルタリング
		if l.f != nil && !l.f(key) {
			continue
		}
		keys = append(keys, key)
		ents = append(ents, constructor())
	}
	if len(keys) == 0 {
		return ents, "", nil // 0件ならすぐに返す
	}
	// カーソルの取得
	newCur := ""
	if len(keys) == limit {
		cursor, err := itr.Cursor()
		if err != nil {
			return nil, "", err
		}
		if _, err := itr.Next(nil); !errors.Is(err, iterator.Done) {
			newCur = cursor.String() // limit+1件目のエンティティがあるのでカーソルが必要
		}
	}
	// エンティティ取得
	anys := toAnySlice(ents)
	err := GetMulti(ctx, keys, anys)
	if err != nil {
		return nil, "", err
	}
	return ents, newCur, nil
}

func (l *EntityLister[E]) GetKeyList(ctx context.Context, limit int, cur string) ([]*datastore.Key, string, error) {
	q := l.q.KeysOnly()
	if cur != "" {
		cursor, err := datastore.DecodeCursor(cur)
		if err != nil {
			return nil, "", err
		}
		q = q.Start(cursor)
	}
	itr := client.Run(ctx, q)
	var keys []*datastore.Key
	// キーの取得
	for len(keys) < limit { // limit件数分取得
		key, err := itr.Next(nil)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, "", err
		}
		// フィルタリング
		if l.f != nil && !l.f(key) {
			continue
		}
		keys = append(keys, key)
	}
	// カーソルの取得
	newCur := ""
	if len(keys) == limit {
		cursor, err := itr.Cursor()
		if err != nil {
			return nil, "", err
		}
		if _, err := itr.Next(nil); !errors.Is(err, iterator.Done) {
			newCur = cursor.String() // limit+1件目のエンティティがあるのでカーソルが必要
		}
	}

	return keys, newCur, nil

}
