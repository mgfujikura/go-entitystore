package entitystore

import (
	"context"
	"errors"
	"log/slog"
	"reflect"

	"cloud.google.com/go/datastore"
	"github.com/samber/lo"

	"go.fujikura.biz/entitystore/cachestore"
)

const LogFormat = "[entitystore] %s"

var client *datastore.Client

var cache cachestore.Cachestore = cachestore.Nostore{}

var logger *slog.Logger

//goland:noinspection GoUnusedExportedFunction
func Client() *datastore.Client {
	return client
}

func EntityToProperties(e any) []datastore.Property {
	var ps []datastore.Property
	var err error
	if ls, ok := e.(datastore.PropertyLoadSaver); ok {
		ps, err = ls.Save()
	} else {
		ps, err = datastore.SaveStruct(e)
	}
	if err != nil {
		panic(err)
	}
	return ps
}

func LoadStruct(ps []datastore.Property, e any) {
	var err error
	if ls, ok := e.(datastore.PropertyLoadSaver); ok {
		err = ls.Load(ps)
	} else {
		err = datastore.LoadStruct(e, ps)
	}
	if err != nil {
		panic(err)
	}
}

// IsProblem err が ErrNoSuchEntity 以外でかつ ErrNoSuchEntity しか含まない MultiError でも無い場合に True を返す
// noinspection GoUnusedExportedFunction
func IsProblem(err error) bool {
	var merr datastore.MultiError
	if err == nil || errors.Is(err, datastore.ErrNoSuchEntity) {
		return false // ErrNoSuchEntity は問題無い
	} else if errors.As(err, &merr) {
		// MultiError の処理
		for _, e := range merr {
			if e != nil && !errors.Is(e, datastore.ErrNoSuchEntity) {
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

	if conf.Cachestore == nil {
		cache = cachestore.Nostore{}
	} else {
		cache = conf.Cachestore
	}
	if conf.Logger == nil {
		logger = slog.Default()
	} else {
		logger = conf.Logger
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
	return Get(ctx, e.Key(), e)
}

func GetEntityMulti[E Entity](ctx context.Context, es []E) error {
	keys := lo.Map(es, func(e E, _ int) *datastore.Key {
		return e.Key()
	})
	anys := toAnySlice(es)
	return GetMulti(ctx, keys, anys)
}

func PutEntity[E Entity](ctx context.Context, e E) error {
	return Put(ctx, e.Key(), e)
}

func PutEntityMulti[E Entity](ctx context.Context, es []E) error {
	return PutMulti(ctx, lo.Map(es, func(e E, _ int) *datastore.Key {
		return e.Key()
	}), es)
}

func DeleteEntity[E Entity](ctx context.Context, e E) error {
	return Delete(ctx, e.Key())
}

func DeleteEntityMulti[E Entity](ctx context.Context, es []E) error {
	return DeleteMulti(ctx, lo.Map(es, func(e E, _ int) *datastore.Key {
		return e.Key()
	}))
}

func GetEntityAll[E Entity](ctx context.Context, q *datastore.Query, dst *[]E) error {
	keys, err := client.GetAll(ctx, q.KeysOnly(), nil)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	*dst = make([]E, len(keys))
	var e E
	var constructor = entityConstructor(e)
	for i := range *dst {
		(*dst)[i] = constructor()
	}
	anys := toAnySlice(*dst)
	return GetMulti(ctx, keys, anys)
}

func GetEntityFirst[E Entity](ctx context.Context, q *datastore.Query, dst E) error {
	q = q.KeysOnly()
	it := client.Run(ctx, q.Limit(1))
	key, err := it.Next(nil)
	if err != nil {
		return err
	}
	return Get(ctx, key, dst)
}

//goland:noinspection GoUnusedExportedFunction
func DeleteCacheByEntities[E Entity](ctx context.Context, es []E) error {
	cacheKeys := lo.Map(es, func(e E, _ int) datastore.Key {
		return *e.Key()
	})
	return cache.DeleteEntities(ctx, cacheKeys)
}

//goland:noinspection GoUnusedExportedFunction
func DeleteCacheByKeys(ctx context.Context, keys []*datastore.Key) error {
	cacheKeys := lo.Map(keys, func(key *datastore.Key, _ int) datastore.Key {
		return *key
	})
	return cache.DeleteEntities(ctx, cacheKeys)
}

func toAnySlice[E any](es []E) []any {
	anys := make([]any, len(es))
	for i, e := range es {
		anys[i] = e
	}
	return anys
}

func entityConstructor[E Entity](e E) func() E {
	t := reflect.TypeOf(e)
	return func() E {
		var v reflect.Value
		if t.Kind() == reflect.Ptr {
			v = reflect.New(t.Elem())
		} else {
			v = reflect.Zero(t)
		}
		return v.Interface().(E)
	}
}
