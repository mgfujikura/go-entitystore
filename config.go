package entitystore

import (
	"google.golang.org/api/option"

	"go.fujikura.biz/entitystore/cachestore"
)

//goland:noinspection GoUnusedConst
const DefaultDatabaseId = "(default)"

type Config struct {
	DatabaseId string
	Options    []option.ClientOption
	Cachestore cachestore.Cachestore
}
