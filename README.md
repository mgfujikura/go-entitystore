# go-entitystore

Cloud Datastore 操作用パッケージです。

ジェネリクスを使った型安全な Entity API、キャッシュ（Cache-aside）、トランザクション、集計、ページングなどをまとめています。

## 主な機能

- `Entity` / `EntityBase` … `UpdatedAt`・`SchemaVersion` 付きのエンティティ契約
- CRUD … `GetEntity` / `PutEntity` / `DeleteEntity` および Multi 版
- キャッシュ … `cachestore.Cachestore`（未設定時は無効）。Put / Delete / Mutate 後に無効化
- トランザクション … `RunInTransaction` + `*Transaction`（Entity API・コミット後のキャッシュ無効化）
- Mutation … `MutateEntity`（Insert / Update / Upsert / Delete、`PrePutAction` あり）
- クエリ補助 … `EntityLister`（カーソルページング）、`GetEntityAll`（`MultiOpLimit` 件ずつ取得）
- 集計 … `Count` / `Avg` / `IntSum` / `Float64Sum` / `NewAggregation`

## 設計上の前提

### パッケージレベルのグローバル状態

`Initialize` で `client` / `cache` / `logger` をパッケージ変数に保持し、`GetEntity(ctx, e)` のようなパッケージ関数 API を提供しています。

これは **ジェネリクス導入を優先した設計**です。メソッドへの型パラメータが使えるようになるまでは、依存を引数や `context` に載せず、この形を維持します。

### SchemaVersion

`SchemaVersion` / `CurrentSchemaVersion` はマイグレーション用の枠です。Datastore には組み込みのスキーマ移行が無いため、バージョンをエンティティに持たせておかないと後からの移行が面倒になります。

本パッケージは自動マイグレーションは行いません。保存時に `CurrentSchemaVersion` を書き込むところまでを担い、実体の移行はアプリケーション側で行ってください。

### キャッシュとエラー

| 操作 | キャッシュ失敗時 |
|------|------------------|
| Get 系の Get / Set | 警告ログのみ。Datastore へフォールバック（または取得結果はそのまま返す） |
| Put / Delete / Mutate 後の無効化 | Datastore は成功済み。`ErrCacheInvalidate` でラップして返す |

```go
err := entitystore.PutEntity(ctx, e)
if errors.Is(err, entitystore.ErrCacheInvalidate) {
    // 保存は成功。キャッシュ無効化だけ失敗
} else if err != nil {
    // Datastore 側の失敗
}
```

トランザクション内の読み取りはキャッシュを使いません。書き込み・削除のキャッシュ無効化は **コミット成功後** に行います。

### EntityLister.WithFilter

`WithFilter` は Datastore のクエリでは表現できないような、キーに対する特殊な絞り込みをクライアント側で行うためのものです。

Iterator でキーを読み進めながらフィルタを適用するため、条件が厳しいと `limit` 件揃うまでに多くの読み取りが発生します。クエリで表現できる条件は、できるだけ `Query` 側（`FilterField` など）に寄せてください。

## 使い方

### 初期化

```go
entitystore.Initialize(ctx, "my-project", entitystore.Config{
    // DatabaseId: "other-db", // 省略時はデフォルト DB
    Cachestore: aememcachestore.NewCachestoreWithTTL(10 * time.Minute), // 省略時はキャッシュなし
    // Logger: slog.Default(),
})
```

### エンティティ定義

```go
type User struct {
    entitystore.EntityBase
    ID   string
    Name string
}

func (u *User) Key() *datastore.Key {
    return datastore.NameKey("User", u.ID, nil)
}

// 必要なら CurrentSchemaVersion をオーバーライド
func (u *User) CurrentSchemaVersion() int { return 1 }
```

### CRUD

```go
var user User
err := entitystore.GetEntity(ctx, &user)

user.Name = "updated"
err = entitystore.PutEntity(ctx, &user)

err = entitystore.DeleteEntity(ctx, &user)
```

### トランザクション

```go
_, err := entitystore.RunInTransaction(ctx, func(tx *entitystore.Transaction) error {
    var user User
    if err := entitystore.GetEntityTx(tx, &user); err != nil {
        return err
    }
    user.Name = "updated"
    return entitystore.PutEntityTx(ctx, tx, &user)
})
```

### キャッシュストア

| 実装 | 用途 |
|------|------|
| （未設定 / `Nostore`） | キャッシュなし |
| `cachestore.Memorystore` | プロセス内（主にテスト） |
| `aememcachestore` | App Engine Memcache。`NewCachestoreWithTTL` で TTL 指定可 |

## テスト

Datastore エミュレータでは本ライブラリの結合テストを十分に回せないため、実 GCP 前提のテストを残しています。CI では単体のみを回す想定です。

### 単体テスト（CI / 日常）

GCP 不要です。GitHub Actions の CI でもこちらを実行します。

```bash
go test ./... -short
```

### 結合テスト（実 GCP Datastore）

`testing.Short()` ではスキップされます。

```bash
go test ./...
```

準備:

1. プロジェクトルートに `service-account-key.json` を配置
2. デフォルトデータベースと `test-database` を用意し、接続確認用エンティティを投入（内容はテスト参照）
3. `./index.yaml` でインデックスを設定

### aememcachestore

App Engine Memcache はローカルでは動かないため、`aememcachestore/test` を App Engine にデプロイして確認します。詳細は `aememcachestore/test/README.md` を参照してください。
