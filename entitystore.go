package entitystore

import (
	"context"

	"cloud.google.com/go/datastore"

	"go.fujikura.biz/entitystore/cachestore"
)

var client *datastore.Client

var cache cachestore.Cachestore = cachestore.Nostore{}

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
	}
}
