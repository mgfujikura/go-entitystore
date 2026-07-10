# go-entitystore
Cloud Datastore 操作用パッケージ


## テストについて

テストは次の2層に分かれています。

### 単体テスト（CI / `go test -short`）

GCP を使わないテストです。PR やローカルの日常確認ではこちらを実行します。

```bash
go test ./... -short
```

（ローカルに `aememcachestore/test` がある場合は echo 依存で失敗することがあるため、そのディレクトリは除外してください。）

### 結合テスト（実 GCP Datastore）

Datastore の実挙動を確認するテストです。`testing.Short()` ではスキップされます。

```bash
go test ./...
```

実行前の準備:

1. プロジェクトルートにサービスアカウントのキーファイルを `service-account-key.json` という名前で配置する
2. テストに使用する Datastore にはデフォルトデータベースと `test-database` が必要
3. それぞれに接続確認用のエンティティを作成しておく（内容はテストを参照）
4. インデックスが必要なため、`./index.yaml` を使ってインデックスを設定しておく
