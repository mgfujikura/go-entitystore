package entitystore

import (
	"context"

	"cloud.google.com/go/datastore"
	"github.com/samber/lo"
)

// MutationType はエンティティの変更タイプを表します。
type MutationType = int

const (
	MutationTypeDelete MutationType = iota
	MutationTypeInsert
	MutationTypeUpdate
	MutationTypeUpsert
)

// Mutation はエンティティの変更を表します。
// 変更の種類と対象のエンティティを保持します。
// MutateEntity関数で使用されます。
type Mutation struct {
	Type   MutationType
	Key    *datastore.Key
	Entity Entity
}

// NewDelete は削除用のMutationを作成します。
func NewDelete[E Entity](e E) *Mutation {
	return &Mutation{MutationTypeDelete, e.Key(), nil}
}

// NewInsert は新規作成用のMutationを作成します。
func NewInsert[E Entity](e E) *Mutation {
	return &Mutation{MutationTypeInsert, e.Key(), e}
}

// NewUpdate は更新用のMutationを作成します。
func NewUpdate[E Entity](e E) *Mutation {
	return &Mutation{MutationTypeUpdate, e.Key(), e}
}

// NewUpsert は新規作成または更新用のMutationを作成します。
func NewUpsert[E Entity](e E) *Mutation {
	return &Mutation{MutationTypeUpsert, e.Key(), e}
}

// MutateEntity は複数のエンティティに対して変更を適用します。
// 引数として渡されたMutationのリストに基づいて、Datastoreに対して一括で変更を行います。
// Insert / Update / Upsert の場合は PutEntity と同様に PrePutAction を実行します。
// 変更後、キャッシュから該当エンティティを削除します。
// Datastore への変更に成功しキャッシュの削除に失敗した場合は ErrCacheInvalidate でラップしたエラーを返します。
func MutateEntity(ctx context.Context, muts ...*Mutation) error {
	for _, m := range muts {
		switch m.Type {
		case MutationTypeInsert, MutationTypeUpdate, MutationTypeUpsert:
			if err := m.Entity.PrePutAction(ctx); err != nil {
				return err
			}
		}
	}
	_, err := client.Mutate(ctx, lo.Map(muts, func(m *Mutation, _ int) *datastore.Mutation {
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
	})...)
	if err != nil {
		return err
	}
	return wrapCacheInvalidate(cache.DeleteEntities(ctx, lo.Map(muts, func(m *Mutation, _ int) datastore.Key {
		return *m.Key
	})))
}
