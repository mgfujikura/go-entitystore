package entitystore

import (
	"context"
	"errors"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"
)

// EntityLister はエンティティのリストの取得を容易にするための構造体です。
type EntityLister[E Entity] struct {
	e E
	q *datastore.Query
	f func(*datastore.Key) bool
}

// NewEntityLister コンストラクタ
func NewEntityLister[E Entity](q *datastore.Query, e E) *EntityLister[E] {
	return &EntityLister[E]{
		e: e,
		q: q,
	}
}

// WithFilter はフィルタ関数を設定します。
// フィルタ関数は各エンティティのキーを受け取り、trueを返した場合にそのエンティティが結果に含まれます。
// フィルタを追加していない場合は、すべてのエンティティが結果に含まれます。
func (l *EntityLister[E]) WithFilter(f func(*datastore.Key) bool) *EntityLister[E] {
	l.f = f
	return l
}

// GetList はエンティティのリストを取得します。
// limitは取得するエンティティの最大数を指定します。
// curは前回の取得時のカーソル文字列を指定します。最初の取得時には空文字列を指定します。
// 戻り値として、取得したエンティティのスライス、新しいカーソル文字列、エラーを返します。
// カーソル文字列はリストに続きがある場合に新しい文字列が返され、
// リストの終わりまで達した際には空文字列が返されます。
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

// GetKeyList はエンティティのキーのリストを取得します。
// キーのリストを返すこと以外は EntityLister.GetList と同様に動作します。
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
