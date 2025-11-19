package entitystore

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/datastore"
	"github.com/samber/lo"
)

// Get は単一のエンティティを取得します。
// キャッシュに存在する場合はキャッシュから取得し、存在しない場合はDatastoreから取得します。
// 取得後、Datastoreから取得した場合はキャッシュに保存します。
func Get(ctx context.Context, key *datastore.Key, dst any) error {
	// キャッシュから取得
	cacheKeys := []datastore.Key{*key}
	cached, err := cache.GetEntities(ctx, cacheKeys)
	if err == nil {
		// キャッシュにあった場合はそれを返す
		if len(cached) > 0 {
			if ps, ok := cached[*key]; ok {
				LoadStruct(ps, dst)
				return nil
			}
		}
	} else {
		// キャッシュのエラーは警告ログを出すだけにする
		logger.Warn(
			fmt.Sprintf(LogFormat, "GetEntity cache.GetEntities error: %v"),
		)
	}
	// キャッシュから取得出来なければ Datastore から取得
	err = client.Get(ctx, key, dst)
	if IsProblem(err) {
		return err
	}
	if errors.Is(err, datastore.ErrNoSuchEntity) {
		return err // エンティティなし
	}
	// 取得したエンティティをキャッシュ
	err = cache.SetEntities(ctx, map[datastore.Key][]datastore.Property{
		*key: EntityToProperties(dst),
	})
	if err != nil {
		// キャッシュのエラーは警告ログを出すだけにする
		logger.Warn(
			fmt.Sprintf(LogFormat, "GetEntity cache.SetEntities error: %v"),
		)
	}
	return nil
}

// GetMulti は複数のエンティティを取得します。
// キャッシュに存在するエンティティはキャッシュから取得し、存在しないエンティティはDatastoreから取得します。
// 取得後、Datastoreから取得したエンティティはキャッシュに保存します。
func GetMulti(ctx context.Context, keys []*datastore.Key, dst []any) error {
	// キャッシュから取得
	cacheKeys := lo.Map(keys, func(key *datastore.Key, _ int) datastore.Key {
		return *key
	})
	cached, err := cache.GetEntities(ctx, cacheKeys)
	if err == nil {
		// キャッシュにあった分をセット
		for i, key := range keys {
			if ps, ok := cached[*key]; ok {
				LoadStruct(ps, dst[i])
			}
		}
		if len(cached) == len(keys) {
			// すべてキャッシュにあった場合は終了
			return nil
		}
	} else {
		// キャッシュのエラーは警告ログを出すだけにする
		logger.Warn(
			fmt.Sprintf(LogFormat, "GetEntityMulti cache.GetEntities error: %v"),
		)
	}
	noerr := false
	var merr datastore.MultiError
	var hits map[datastore.Key][]datastore.Property
	if len(cached) == 0 {
		// まったくキャッシュに無かった場合、全て Datastore から取得
		err = client.GetMulti(ctx, keys, dst)
		if IsProblem(err) {
			return err
		}
		if err == nil {
			noerr = true
			err = make(datastore.MultiError, len(keys))
		} else {
			errors.As(err, &merr)
		}
		// キャッシュするデータを準備
		hits = make(map[datastore.Key][]datastore.Property, len(keys))
		for i, e := range err.(datastore.MultiError) {
			if e == nil {
				hits[*keys[i]] = EntityToProperties(dst[i])
			}
		}
	} else {
		// 一部キャッシュにあった場合
		noCacheKeys := make([]*datastore.Key, 0, len(keys))
		noCaches := make([]any, 0, len(keys))
		for i, key := range keys {
			if _, ok := cached[*key]; !ok {
				noCacheKeys = append(noCacheKeys, key)
				noCaches = append(noCaches, dst[i])
			}
		}
		// キャッシュに無いものだけ Datastore から取得
		err = client.GetMulti(ctx, noCacheKeys, noCaches)
		if IsProblem(err) {
			return err
		}
		if err == nil {
			noerr = true
			err = make(datastore.MultiError, len(noCaches))
		}
		// 結果を元のスライスにセット
		merr = make(datastore.MultiError, len(keys))
		hits = make(map[datastore.Key][]datastore.Property, len(keys))
		p := 0
		for i, e := range err.(datastore.MultiError) {
			for ; p < len(keys); p++ {
				if *keys[p] == *noCacheKeys[i] {
					if e == nil {
						dst[p] = noCaches[i]
						hits[*keys[p]] = EntityToProperties(dst[i])
						break
					} else {
						merr[p] = e
					}
				}
			}
		}
	}
	// キャッシュ
	cacheErr := cache.SetEntities(ctx, hits)
	if cacheErr != nil {
		// キャッシュのエラーは警告ログを出すだけにする
		logger.Warn(
			fmt.Sprintf(LogFormat, "GetEntityMulti cache.SetEntities error: %v"),
		)
	}
	if noerr {
		return nil
	}
	return merr
}

// Put は単一のエンティティをDatastoreに保存します。
// 保存後、キャッシュを削除します。
func Put(ctx context.Context, key *datastore.Key, src any) error {
	_, err := client.Put(ctx, key, src)
	if err != nil {
		return err
	}
	return cache.DeleteEntities(ctx, []datastore.Key{*key})
}

// PutMulti は複数のエンティティをDatastoreに一括保存します。
// 保存後、キャッシュを削除します。
func PutMulti(ctx context.Context, keys []*datastore.Key, src any) error {
	_, err := client.PutMulti(ctx, keys, src)
	if err != nil {
		return err
	}
	return cache.DeleteEntities(ctx, lo.Map(keys, func(key *datastore.Key, _ int) datastore.Key {
		return *key
	}))
}

// Delete は単一のエンティティをDatastoreとキャッシュから削除します。
func Delete(ctx context.Context, key *datastore.Key) error {
	err := client.Delete(ctx, key)
	if err != nil {
		return err
	}
	return cache.DeleteEntities(ctx, []datastore.Key{*key})
}

// DeleteMulti は複数のエンティティをDatastoreとキャッシュから一括削除します。
func DeleteMulti(ctx context.Context, keys []*datastore.Key) error {
	err := client.DeleteMulti(ctx, keys)
	if err != nil {
		return err
	}
	return cache.DeleteEntities(ctx, lo.Map(keys, func(key *datastore.Key, _ int) datastore.Key {
		return *key
	}))
}

// Run は client.Run のラッパーです。
// 特別な処理は行いません。
//
//goland:noinspection GoUnusedExportedFunction
func Run(ctx context.Context, q *datastore.Query) *datastore.Iterator {
	return client.Run(ctx, q)
}

// RunInTransaction は client.RunInTransaction のラッパーです。
// 特別な処理は行いません。
//
//goland:noinspection GoUnusedExportedFunction
func RunInTransaction(ctx context.Context, f func(tx *datastore.Transaction) error, opts ...datastore.TransactionOption) (cmt *datastore.Commit, err error) {
	return client.RunInTransaction(ctx, f, opts...)
}
