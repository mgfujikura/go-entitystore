# aememcachestore

`google.golang.org/appengine/memcache` を利用した `cachestore.Cachestore` の実装です。

## テストについて

`google.golang.org/appengine/memcache` はローカル環境で動作しないため、  
aememcachestore のテストは App Engine 上で実行する必要があります。

そのため、aememcachestore のテストは実際に App Engine にデプロイして実行します。

`aememcachestore/test/env.cmd.sample` を参考に `env.cmd` を作成し、
テスト用プロジェクト名を設定してください。

### デプロイ

```cmd
aememcachestore\test\deploy.cmd
```

### テスト実行

```cmd
gcloud app browse --project=entitystore-test-project
```
