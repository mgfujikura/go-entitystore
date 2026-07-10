package entitystore

import (
	"context"

	"cloud.google.com/go/datastore"
	"github.com/samber/lo"
)

// Transaction は Datastore トランザクションを Entity / キャッシュ連携付きで扱うラッパーです。
// トランザクション内の読み取りはキャッシュを使わず、書き込み・削除のキャッシュ無効化はコミット成功後に行います。
type Transaction struct {
	tx           *datastore.Transaction
	keys         []datastore.Key
	pendingKeys  []*datastore.PendingKey
}

// RunInTransaction はトランザクションを実行します。
// f が成功してコミットされたあと、トランザクション内で変更したキーのキャッシュを無効化します。
// キャッシュ無効化に失敗した場合は ErrCacheInvalidate でラップしたエラーを返します（コミット自体は成功しています）。
func RunInTransaction(ctx context.Context, f func(tx *Transaction) error, opts ...datastore.TransactionOption) (*datastore.Commit, error) {
	var tx *Transaction
	cmt, err := client.RunInTransaction(ctx, func(dtx *datastore.Transaction) error {
		tx = &Transaction{tx: dtx}
		return f(tx)
	}, opts...)
	if err != nil {
		return cmt, err
	}
	if tx == nil {
		return cmt, nil
	}
	return cmt, tx.invalidateCache(ctx, cmt)
}

// Raw は内部の *datastore.Transaction を返します。
func (t *Transaction) Raw() *datastore.Transaction {
	return t.tx
}

// Query はトランザクションに紐づく Query を返します。
func (t *Transaction) Query(kind string) Query {
	return NewQuery(kind).Transaction(t.tx)
}

// Get はトランザクション内でエンティティを取得します。キャッシュは使用しません。
func (t *Transaction) Get(key *datastore.Key, dst any) error {
	return t.tx.Get(key, dst)
}

// GetMulti はトランザクション内で複数のエンティティを取得します。キャッシュは使用しません。
func (t *Transaction) GetMulti(keys []*datastore.Key, dst any) error {
	return t.tx.GetMulti(keys, dst)
}

// Put はトランザクション内でエンティティを保存します。
// キャッシュの無効化はコミット成功後に行われます。
func (t *Transaction) Put(key *datastore.Key, src any) error {
	pk, err := t.tx.Put(key, src)
	if err != nil {
		return err
	}
	t.trackPut(key, pk)
	return nil
}

// PutMulti はトランザクション内で複数のエンティティを保存します。
// キャッシュの無効化はコミット成功後に行われます。
func (t *Transaction) PutMulti(keys []*datastore.Key, src any) error {
	pks, err := t.tx.PutMulti(keys, src)
	if err != nil {
		return err
	}
	for i, key := range keys {
		t.trackPut(key, pks[i])
	}
	return nil
}

// Delete はトランザクション内でエンティティを削除します。
// キャッシュの無効化はコミット成功後に行われます。
func (t *Transaction) Delete(key *datastore.Key) error {
	if err := t.tx.Delete(key); err != nil {
		return err
	}
	t.trackKey(key)
	return nil
}

// DeleteMulti はトランザクション内で複数のエンティティを削除します。
// キャッシュの無効化はコミット成功後に行われます。
func (t *Transaction) DeleteMulti(keys []*datastore.Key) error {
	if err := t.tx.DeleteMulti(keys); err != nil {
		return err
	}
	for _, key := range keys {
		t.trackKey(key)
	}
	return nil
}

// Mutate はトランザクション内で Mutation を適用します。
// 返された PendingKey のうち incomplete なキー由来のものはコミット後のキャッシュ無効化に使われます。
// 完成済みキーのキャッシュ無効化は呼び出し側で track するか、MutateEntityTx を使ってください。
func (t *Transaction) Mutate(muts ...*datastore.Mutation) ([]*datastore.PendingKey, error) {
	pks, err := t.tx.Mutate(muts...)
	if err != nil {
		return nil, err
	}
	for _, pk := range pks {
		if pk != nil {
			t.pendingKeys = append(t.pendingKeys, pk)
		}
	}
	return pks, nil
}

// GetEntity はトランザクション内で単一のエンティティを取得します。キャッシュは使用しません。
func GetEntityTx[E Entity](tx *Transaction, e E) error {
	return tx.Get(e.Key(), e)
}

// GetEntityMultiTx はトランザクション内で複数のエンティティを取得します。キャッシュは使用しません。
func GetEntityMultiTx[E Entity](tx *Transaction, es []E) error {
	keys := lo.Map(es, func(e E, _ int) *datastore.Key {
		return e.Key()
	})
	return tx.GetMulti(keys, toAnySlice(es))
}

// PutEntityTx はトランザクション内で単一のエンティティを保存します。
// PrePutAction を実行し、キャッシュの無効化はコミット成功後に行われます。
func PutEntityTx[E Entity](ctx context.Context, tx *Transaction, e E) error {
	if err := e.PrePutAction(ctx); err != nil {
		return err
	}
	return tx.Put(e.Key(), e)
}

// PutEntityMultiTx はトランザクション内で複数のエンティティを保存します。
// PrePutAction を実行し、キャッシュの無効化はコミット成功後に行われます。
func PutEntityMultiTx[E Entity](ctx context.Context, tx *Transaction, es []E) error {
	keys := make([]*datastore.Key, 0, len(es))
	for _, e := range es {
		if err := e.PrePutAction(ctx); err != nil {
			return err
		}
		keys = append(keys, e.Key())
	}
	return tx.PutMulti(keys, es)
}

// DeleteEntityTx はトランザクション内で単一のエンティティを削除します。
// キャッシュの無効化はコミット成功後に行われます。
func DeleteEntityTx[E Entity](tx *Transaction, e E) error {
	return tx.Delete(e.Key())
}

// DeleteEntityMultiTx はトランザクション内で複数のエンティティを削除します。
// キャッシュの無効化はコミット成功後に行われます。
func DeleteEntityMultiTx[E Entity](tx *Transaction, es []E) error {
	return tx.DeleteMulti(lo.Map(es, func(e E, _ int) *datastore.Key {
		return e.Key()
	}))
}

// MutateEntityTx はトランザクション内で複数のエンティティに対して変更を適用します。
// Insert / Update / Upsert の場合は PrePutAction を実行します。
// キャッシュの無効化はコミット成功後に行われます。
func MutateEntityTx(ctx context.Context, tx *Transaction, muts ...*Mutation) error {
	for _, m := range muts {
		switch m.Type {
		case MutationTypeInsert, MutationTypeUpdate, MutationTypeUpsert:
			if err := m.Entity.PrePutAction(ctx); err != nil {
				return err
			}
		}
	}
	dsMuts := lo.Map(muts, func(m *Mutation, _ int) *datastore.Mutation {
		switch m.Type {
		case MutationTypeDelete:
			return datastore.NewDelete(m.Key)
		case MutationTypeInsert:
			return datastore.NewInsert(m.Key, m.Entity)
		case MutationTypeUpdate:
			return datastore.NewUpdate(m.Key, m.Entity)
		case MutationTypeUpsert:
			return datastore.NewUpsert(m.Key, m.Entity)
		default:
			panic("unknown mutation type")
		}
	})
	if _, err := tx.Mutate(dsMuts...); err != nil {
		return err
	}
	for _, m := range muts {
		tx.trackKey(m.Key)
	}
	return nil
}

func (t *Transaction) trackPut(key *datastore.Key, pk *datastore.PendingKey) {
	if key.Incomplete() {
		t.pendingKeys = append(t.pendingKeys, pk)
		return
	}
	t.trackKey(key)
}

func (t *Transaction) trackKey(key *datastore.Key) {
	if key == nil || key.Incomplete() {
		return
	}
	t.keys = append(t.keys, *key)
}

func (t *Transaction) invalidateCache(ctx context.Context, cmt *datastore.Commit) error {
	keys := t.keys
	if cmt != nil {
		for _, pk := range t.pendingKeys {
			if k := cmt.Key(pk); k != nil {
				keys = append(keys, *k)
			}
		}
	}
	if len(keys) == 0 {
		return nil
	}
	// 重複排除
	uniq := make(map[string]datastore.Key, len(keys))
	for _, k := range keys {
		uniq[k.Encode()] = k
	}
	list := make([]datastore.Key, 0, len(uniq))
	for _, k := range uniq {
		list = append(list, k)
	}
	return wrapCacheInvalidate(cache.DeleteEntities(ctx, list))
}
