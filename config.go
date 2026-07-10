package entitystore

import (
	"log/slog"

	"google.golang.org/api/option"

	"go.fujikura.biz/entitystore/cachestore"
)

//goland:noinspection GoUnusedConst
const DefaultDatabaseId = "(default)"

// Config は entitystore の初期化設定のための構造体です。
type Config struct {
	DatabaseId string
	Options    []option.ClientOption
	Cachestore cachestore.Cachestore
	Logger     *slog.Logger
}
