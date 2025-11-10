package entitystore

import (
	"context"

	"cloud.google.com/go/datastore"
	"github.com/samber/lo"

	"go.fujikura.biz/entitystore/cachestore"
)

var client *datastore.Client

var cache cachestore.Cachestore = cachestore.Nostore{}

func Client() *datastore.Client {
	return client
}

func EntityToProperties[E Entity](e E) []datastore.Property {
	var ps []datastore.Property
	var err error
	if ls, ok := any(e).(datastore.PropertyLoadSaver); ok {
		ps, err = ls.Save()
	} else {
		ps, err = datastore.SaveStruct(e)
	}
	if err != nil {
		panic(err)
	}
	return ps
}

func LoadStruct[E Entity](ps []datastore.Property, e E) {
	if ls, ok := any(e).(datastore.PropertyLoadSaver); ok {
		ls.Load(ps)
	} else {
		datastore.LoadStruct(e, ps)
	}
}

// IsProbrem err が ErrNoSuchEntity 以外でかつ ErrNoSuchEntity しか含まない MultiError でも無い場合に True を返す
// noinspection GoUnusedExportedFunction
func IsProbrem(err error) bool {
	if err == nil || err == datastore.ErrNoSuchEntity {
		return false // ErrNoSuchEntity は問題無い
	} else if merr, ok := err.(datastore.MultiError); ok {
		// MultiError の処理
		for _, e := range merr {
			if e != nil && e != datastore.ErrNoSuchEntity {
				return true // MultiError に ErrNoSuchEntity 以外が含まれているなら問題あり
			}
		}
		return false // 問題なし
	} else {
		// ErrNoSuchEntity でも MultiError でもないなら問題あり
		return true
	}
}

func Initialize(ctx context.Context, projectId string, conf Config) {
	var err error
	if conf.DatabaseId == "" {
		client, err = datastore.NewClient(ctx, projectId, conf.Options...)
	} else {
		client, err = datastore.NewClientWithDatabase(ctx, projectId, conf.DatabaseId, conf.Options...)
	}
	if err != nil {
		panic(err)
	}

	if conf.Cachestore != nil {
		cache = conf.Cachestore
	} else {
		cache = cachestore.Nostore{}
	}
}

func DeleteAll(ctx context.Context, kind string) error {
	// クエリで対象の Kind のすべてのキーを取得
	query := datastore.NewQuery(kind).KeysOnly()
	keys, err := client.GetAll(ctx, query, nil)
	if err != nil {
		return err
	}
	// Datastore API の制限により、最大500件ずつ削除
	const batchSize = 500
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}

		if err := client.DeleteMulti(ctx, keys[i:end]); err != nil {
			return err
		}
	}

	return nil
}

func GetEntity[E Entity](ctx context.Context, e E) error {
	cached, err := cache.GetEntities(ctx, []datastore.Key{*e.Key()})
	if err != nil {
		return err
	}
	if ps, ok := cached[*e.Key()]; ok {
		LoadStruct(ps, e)
		return nil
	}
	return client.Get(ctx, e.Key(), e)
}

func GetEntityMulti[E Entity](ctx context.Context, es []E) error {
	// キャッシュから取得
	keys := lo.Map(es, func(e E, _ int) datastore.Key {
		return *e.Key()
	})
	cached, err := cache.GetEntities(ctx, keys)
	if err != nil {
		return err
	}
	// キャッシュにあった分をセット
	for _, e := range es {
		if ps, ok := cached[*e.Key()]; ok {
			LoadStruct(ps, e)
		}
	}
	if len(cached) == len(es) {
		// すべてキャッシュにあった場合は終了
		return nil
	}
	var merr datastore.MultiError
	if len(cached) == 0 {
		// まったくキャッシュに無かった場合、全て Datastore から取得
		keys := lo.Map(es, func(e E, _ int) *datastore.Key {
			return e.Key()
		})
		err = client.GetMulti(ctx, keys, es)
		if IsProbrem(err) {
			return err
		}
		if err == nil {
			err = make(datastore.MultiError, len(es))
		}
		// キャッシュ
		hits := make(map[datastore.Key][]datastore.Property, len(es))
		for i, e := range err.(datastore.MultiError) {
			if e == nil {
				hits[*es[i].Key()] = EntityToProperties(es[i])
			}
		}
		return cache.SetEntities(ctx, hits)
	} else {
		// 一部キャッシュにあった場合
		noCacheKeys := make([]*datastore.Key, 0, len(es))
		noCaches := make([]E, 0, len(es))
		for _, e := range es {
			if ps, ok := cached[*e.Key()]; ok {
				// キャッシュにあった分をセット
				LoadStruct(ps, e)
			} else {
				noCacheKeys = append(noCacheKeys, e.Key())
				noCaches = append(noCaches, e)
			}
		}
		// キャッシュに無いものだけ Datastore から取得
		err = client.GetMulti(ctx, noCacheKeys, noCaches)
		if IsProbrem(err) {
			return err
		}
		if err == nil {
			err = make(datastore.MultiError, len(noCaches))
		}
		// 結果を元のスライスにセット
		merr = make(datastore.MultiError, len(es))
		hits := make(map[datastore.Key][]datastore.Property, len(es))
		p := 0
		for i, e := range err.(datastore.MultiError) {
			for ; p < len(es); p++ {
				if *es[p].Key() == *noCacheKeys[i] {
					if e == nil {
						es[p] = noCaches[i]
						hits[*es[p].Key()] = EntityToProperties(es[i])
						break
					} else {
						merr[i] = e
					}
				}
			}
		}
		// キャッシュ
		return cache.SetEntities(ctx, hits)
	}
}

func PutEntity[E Entity](ctx context.Context, e E) error {
	return nil
}

func PutEntityMulti[E Entity](ctx context.Context, es []E) error {
	return nil
}

func DeleteEntity[E Entity](ctx context.Context, e E) error {
	return nil
}

func DeleteEntityMulti[E Entity](ctx context.Context, es []E) error {
	return nil
}

func GetEntityAll[E Entity](ctx context.Context, q datastore.Query, dst []E) error {
	return nil
}

func GetEntityFirst[E Entity](ctx context.Context, q datastore.Query, dst E) error {
	return nil
}

func MutateEntity(ctx context.Context, muts ...*Mutation) error {
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
	ents := make(map[datastore.Key][]datastore.Property)
	for _, m := range muts {
		if m.Type == MutationTypeDelete {
			continue
		}
		ents[*m.Key] = m.Entity
	}
	return cache.SetEntities(ctx, ents)
}

func Run(ctx context.Context, q *datastore.Query) *datastore.Iterator {
	return client.Run(ctx, q)
}

func RunInTransaction(ctx context.Context, f func(tx *datastore.Transaction) error, opts ...datastore.TransactionOption) (cmt *datastore.Commit, err error) {
	return client.RunInTransaction(ctx, f, opts...)
}

func Count(ctx context.Context, q datastore.Query) (int, error) {
	return 0, nil
}
func Avg(ctx context.Context, q datastore.Query) (float64, error) {
	return 0, nil
}
func Sum(ctx context.Context, q datastore.Query) (float64, error) {
	return 0, nil
}
