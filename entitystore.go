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

// LogFormat はログ出力時のフォーマット文字列です。
const LogFormat = "[entitystore] %s"

// client は Datastore クライアントのインスタンスです。
var client *datastore.Client

// cache は Cachestore のインスタンスです。
var cache cachestore.Cachestore = cachestore.Nostore{}

// logger は slog.Logger のインスタンスです。
var logger *slog.Logger

// Client は現在のdatastoreクライアントを返します。
//
//goland:noinspection GoUnusedExportedFunction
func Client() *datastore.Client {
	return client
}

// EntityToProperties はエンティティをdatastoreのプロパティスライスに変換します。
// 変換時にエラーが発生した場合はパニックを起こします。
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

// LoadStruct はdatastoreのプロパティスライスをエンティティにロードします。
// datastore.PropertyLoadSaver を実装している場合はそのエンティティに実装されているLoadメソッドを使用し、
// そうでない場合はdatastore.LoadStructを使用します。
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

// IsProblem はエラーが問題が発生していることを示しているかどうかを判定します。
// err が ErrNoSuchEntity 以外でかつ ErrNoSuchEntity しか含まない MultiError でも無い場合に True を返します。
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

// Initialize は entitystore を初期化します。
// projectId は GCP のプロジェクト ID を指定します。
// conf にはオプションを指定します。
// DatabaseId が空文字列の場合はデフォルトのデータベースが使用されます。
// Cachestore が nil の場合はキャッシュを使用しません。
// Logger が nil の場合はデフォルトの slog.Logger が使用されます。
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

// DeleteAll は指定された Kind のすべてのエンティティを削除します。
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

// GetEntity は単一のエンティティを取得します。
// キャッシュに存在する場合はキャッシュから取得し、存在しない場合はDatastoreから取得します。
// 取得後、Datastoreから取得した場合はキャッシュに保存します。
func GetEntity[E Entity](ctx context.Context, e E) error {
	return Get(ctx, e.Key(), e)
}

// GetEntityMulti は複数のエンティティを一括取得します。
// キャッシュに存在するエンティティはキャッシュから取得し、存在しないエンティティはDatastoreから取得します。
// 取得後、Datastoreから取得したエンティティはキャッシュに保存します。
func GetEntityMulti[E Entity](ctx context.Context, es []E) error {
	keys := lo.Map(es, func(e E, _ int) *datastore.Key {
		return e.Key()
	})
	anys := toAnySlice(es)
	return GetMulti(ctx, keys, anys)
}

// PutEntity は単一のエンティティを保存します。
// 保存後、キャッシュを削除します。
func PutEntity[E Entity](ctx context.Context, e E) error {
	return Put(ctx, e.Key(), e)
}

// PutEntityMulti は複数のエンティティを一括保存します。
// 保存後、キャッシュを削除します。
func PutEntityMulti[E Entity](ctx context.Context, es []E) error {
	return PutMulti(ctx, lo.Map(es, func(e E, _ int) *datastore.Key {
		return e.Key()
	}), es)
}

// DeleteEntity は単一のエンティティをDatastoreとキャッシュから削除します。
func DeleteEntity[E Entity](ctx context.Context, e E) error {
	return Delete(ctx, e.Key())
}

// DeleteEntityMulti は複数のエンティティをDatastoreとキャッシュから一括削除します。
func DeleteEntityMulti[E Entity](ctx context.Context, es []E) error {
	return DeleteMulti(ctx, lo.Map(es, func(e E, _ int) *datastore.Key {
		return e.Key()
	}))
}

// GetEntityAll はクエリにマッチするすべてのエンティティを取得します。
// 取得したエンティティは dst に格納されます。
// キャッシュに存在するエンティティはキャッシュから取得し、存在しないエンティティはDatastoreから取得します。
// 取得後、Datastoreから取得したエンティティはキャッシュに保存します。
// クエリやキーはキャッシュしません。毎回Datastoreに問い合わせ、エンティティの取得のみキャッシュを利用します。
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

// GetEntityFirst はクエリにマッチする最初のエンティティを取得します。
// 最初のエンティティのみを取得すること以外は GetEntityAll と同様に動作します。
func GetEntityFirst[E Entity](ctx context.Context, q *datastore.Query, dst E) error {
	q = q.KeysOnly()
	it := client.Run(ctx, q.Limit(1))
	key, err := it.Next(nil)
	if err != nil {
		return err
	}
	return Get(ctx, key, dst)
}

// DeleteCacheByEntities はキャッシュからエンティティを削除します。
// 通常キャッシュは PutEntity や DeleteEntity 時に自動的に削除されますが、
// それ以外のタイミングでキャッシュを削除したい場合に使用します。
//
//goland:noinspection GoUnusedExportedFunction
func DeleteCacheByEntities[E Entity](ctx context.Context, es []E) error {
	cacheKeys := lo.Map(es, func(e E, _ int) datastore.Key {
		return *e.Key()
	})
	return cache.DeleteEntities(ctx, cacheKeys)
}

// DeleteCacheByKeys はキーを元にキャッシュからエンティティを削除します。
// 通常キャッシュは PutEntity や DeleteEntity 時に自動的に削除されますが、
// それ以外のタイミングでキャッシュを削除したい場合に使用します。
//
//goland:noinspection GoUnusedExportedFunction
func DeleteCacheByKeys(ctx context.Context, keys []*datastore.Key) error {
	cacheKeys := lo.Map(keys, func(key *datastore.Key, _ int) datastore.Key {
		return *key
	})
	return cache.DeleteEntities(ctx, cacheKeys)
}

// toAnySlice は任意の型のスライスを any 型のスライスに変換します。
func toAnySlice[E any](es []E) []any {
	anys := make([]any, len(es))
	for i, e := range es {
		anys[i] = e
	}
	return anys
}

// entityConstructor はエンティティのコンストラクタ関数を生成します。
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
