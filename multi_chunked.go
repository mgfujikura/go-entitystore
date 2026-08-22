package entitystore

import (
	"errors"
	"fmt"

	"cloud.google.com/go/datastore"
)

// chunkedMulti は n 件の操作を limit 件ずつに分割して fn を実行します。
// fn が MultiError を返した場合は全体の MultiError にマージし、それ以外のエラーはそのまま返します。
func chunkedMulti(n, limit int, fn func(start, end int) error) error {
	if n == 0 {
		return nil
	}
	if limit <= 0 {
		return fmt.Errorf("entitystore: invalid chunk limit %d", limit)
	}
	var merr datastore.MultiError
	anyMerr := false
	for start := 0; start < n; start += limit {
		end := start + limit
		if end > n {
			end = n
		}
		err := fn(start, end)
		if err == nil {
			continue
		}
		var chunkMerr datastore.MultiError
		if !errors.As(err, &chunkMerr) {
			return err
		}
		if !anyMerr {
			merr = make(datastore.MultiError, n)
			anyMerr = true
		}
		copy(merr[start:end], chunkMerr)
	}
	if anyMerr {
		return merr
	}
	return nil
}
