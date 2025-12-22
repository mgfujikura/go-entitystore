package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/datastore"
	"github.com/samber/lo"
	"google.golang.org/appengine/v2/memcache"

	"go.fujikura.biz/entitystore/aememcachestore"
	"go.fujikura.biz/entitystore/cachestore"
)

type Tests struct{}

func (t *Tests) TestTest() TestResult {
	return TestResult{Name: "Sample Test", OK: true, Messages: []string{"This is a sample test message."}}
}

//goland:noinspection NonAsciiCharacters
func (t *Tests) TestGetEntities1エンティティ取得() *TestResult {
	result := NewTestResult("TestGetEntities_1エンティティ取得")
	ctx := context.Background()

	// 事前に memcache にエンティティをセットしておく
	ps := []datastore.Property{
		{Name: "Name", Value: "Alice"},
		{Name: "Age", Value: 30},
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(ps)
	if err != nil {
		result.AddError(fmt.Errorf("encode error: %v", err))
		return result
	}
	key := *datastore.NameKey("TestGetEntities", "Alice", nil)
	hashKey := aememcachestore.KeyHash(key)
	err = memcache.Set(ctx, &memcache.Item{Key: aememcachestore.Prefix + hashKey, Value: buf.Bytes()})
	if err != nil {
		result.AddError(fmt.Errorf("memcache set error: %v", err))
		return result
	}

	// Cachestore を使ってエンティティを取得
	cs := aememcachestore.NewCachestore()
	keys := []datastore.Key{*datastore.NameKey("TestGetEntities", "Alice", nil)}
	entities, err := cs.GetEntities(ctx, keys)
	if err != nil {
		result.AddError(fmt.Errorf("GetEntities error: %v", err))
		return result
	}
	if len(entities) != 1 {
		result.AddError(fmt.Errorf("expected 1 entity, got %d", len(entities)))
		return result
	}
	retPs, ok := entities[key]
	if !ok {
		result.AddError(fmt.Errorf("entity not found for key: %v, get keys: %v", key, lo.Keys(entities)))
		return result
	}
	if len(retPs) != len(ps) {
		result.AddError(fmt.Errorf("expected %d properties, got %d", len(ps), len(retPs)))
		return result
	}
	// プロパティの内容を確認
	for i, p := range ps {
		if p.Name != retPs[i].Name || p.Value != retPs[i].Value {
			result.AddError(fmt.Errorf("property mismatch at index %d: expected %v, got %v", i, p, retPs[i]))
		}
	}

	return result
}

//goland:noinspection NonAsciiCharacters
func (t *Tests) TestSetEntities1エンティティセット() *TestResult {
	result := NewTestResult("TestSetEntities_1エンティティセット")
	ctx := context.Background()

	key := *datastore.NameKey("TestGetEntities", "Alice", nil)
	ps := []datastore.Property{
		{Name: "Name", Value: "Alice"},
		{Name: "Age", Value: 30},
	}
	items := map[datastore.Key][]datastore.Property{
		key: ps,
	}

	cs := aememcachestore.NewCachestore()
	err := cs.SetEntities(ctx, items)
	if err != nil {
		result.AddError(fmt.Errorf("SetEntities error: %v", err))
		return result
	}
	// GetEntitiesで取得して確認
	entities, err := cs.GetEntities(ctx, []datastore.Key{key})
	if err != nil {
		result.AddError(fmt.Errorf("GetEntities error: %v", err))
		return result
	}
	if len(entities) != 1 {
		result.AddError(fmt.Errorf("expected 1 entity, got %d", len(entities)))
		return result
	}
	retPs, ok := entities[key]
	if !ok {
		result.AddError(fmt.Errorf("entity not found for key: %v, get keys: %v", key, lo.Keys(entities)))
		return result
	}
	if len(retPs) != len(ps) {
		result.AddError(fmt.Errorf("expected %d properties, got %d", len(ps), len(retPs)))
		return result
	}
	// プロパティの内容を確認
	for i, p := range ps {
		if p.Name != retPs[i].Name || p.Value != retPs[i].Value {
			result.AddError(fmt.Errorf("property mismatch at index %d: expected %v, got %v", i, p, retPs[i]))
		}
	}

	return result
}

//goland:noinspection NonAsciiCharacters
func (t *Tests) TestSetEntitiesエンティティサイズエラー() *TestResult {
	result := NewTestResult("TestSetEntities_エンティティサイズエラー")
	ctx := context.Background()

	key := *datastore.NameKey("TestGetEntities", "Alice", nil)
	ps := []datastore.Property{
		{Name: "Name", Value: strings.Repeat("A", 1024*1024)}, // 1MBの文字列
		{Name: "Age", Value: 30},
	}
	items := map[datastore.Key][]datastore.Property{
		key: ps,
	}

	cs := aememcachestore.NewCachestore()
	err := cs.SetEntities(ctx, items)
	if !errors.Is(err, cachestore.ErrCacheSizeOver) {
		result.AddError(fmt.Errorf("SetEntities not error"))
		return result
	}
	return result
}

//goland:noinspection NonAsciiCharacters
func (t *Tests) TestSetGetEntities複数エンティティのセットと取得() *TestResult {
	result := NewTestResult("TestSetGetEntities_複数エンティティのセットと取得")
	ctx := context.Background()

	key1 := *datastore.NameKey("TestGetEntities", "Alice", nil)
	ps1 := []datastore.Property{
		{Name: "Name", Value: "Alice"},
		{Name: "Age", Value: 30},
	}
	key2 := *datastore.NameKey("TestGetEntities", "Bob", nil)
	ps2 := []datastore.Property{
		{Name: "Name", Value: "Bob"},
		{Name: "Age", Value: 45},
	}
	items := map[datastore.Key][]datastore.Property{
		key1: ps1,
		key2: ps2,
	}

	cs := aememcachestore.NewCachestore()
	err := cs.SetEntities(ctx, items)
	if err != nil {
		result.AddError(fmt.Errorf("SetEntities error: %v", err))
		return result
	}
	// GetEntitiesで取得して確認
	entities, err := cs.GetEntities(ctx, []datastore.Key{key1, key2})
	if err != nil {
		result.AddError(fmt.Errorf("GetEntities error: %v", err))
		return result
	}
	retPs, ok := entities[key1]
	if !ok {
		result.AddError(fmt.Errorf("entity not found for key: %v, get keys: %v", key1, lo.Keys(entities)))
		return result
	}
	for i, p := range ps1 {
		if p.Name != retPs[i].Name || p.Value != retPs[i].Value {
			result.AddError(fmt.Errorf("property mismatch at index %d: expected %v, got %v", i, p, retPs[i]))
		}
	}
	retPs, ok = entities[key2]
	if !ok {
		result.AddError(fmt.Errorf("entity not found for key: %v, get keys: %v", key2, lo.Keys(entities)))
		return result
	}
	for i, p := range ps2 {
		if p.Name != retPs[i].Name || p.Value != retPs[i].Value {
			result.AddError(fmt.Errorf("property mismatch at index %d: expected %v, got %v", i, p, retPs[i]))
		}
	}

	return result
}

//goland:noinspection NonAsciiCharacters
func (t *Tests) TestDeleteEntities複数削除() *TestResult {
	result := NewTestResult("TestDeleteEntities_複数削除")
	ctx := context.Background()

	key1 := *datastore.NameKey("TestGetEntities", "Alice", nil)
	ps1 := []datastore.Property{
		{Name: "Name", Value: "Alice"},
		{Name: "Age", Value: 30},
	}
	key2 := *datastore.NameKey("TestGetEntities", "Bob", nil)
	ps2 := []datastore.Property{
		{Name: "Name", Value: "Bob"},
		{Name: "Age", Value: 45},
	}
	key3 := *datastore.NameKey("TestGetEntities", "Carol", nil)
	ps3 := []datastore.Property{
		{Name: "Name", Value: "Bob"},
		{Name: "Age", Value: 22},
	}
	items := map[datastore.Key][]datastore.Property{
		key1: ps1,
		key2: ps2,
		key3: ps3,
	}
	cs := aememcachestore.NewCachestore()
	err := cs.SetEntities(ctx, items)
	if err != nil {
		result.AddError(fmt.Errorf("SetEntities error: %v", err))
		return result
	}

	err = cs.DeleteEntities(ctx, []datastore.Key{key1, key3})
	if err != nil {
		result.AddError(fmt.Errorf("DeleteEntities error: %v", err))
		return result
	}

	// GetEntitiesで取得して確認
	entities, err := cs.GetEntities(ctx, []datastore.Key{key1, key2, key3, *datastore.NameKey("TestGetEntities", "Dave", nil)})
	if err != nil {
		result.AddError(fmt.Errorf("GetEntities error: %v", err))
		return result
	}
	if len(entities) != 1 {
		result.AddError(fmt.Errorf("expected 1 entity, got %d", len(entities)))
		return result
	}
	retPs, ok := entities[key2]
	if !ok {
		result.AddError(fmt.Errorf("entity not found for key: %v, get keys: %v", key2, lo.Keys(entities)))
		return result
	}
	for i, p := range ps2 {
		if p.Name != retPs[i].Name || p.Value != retPs[i].Value {
			result.AddError(fmt.Errorf("property mismatch at index %d: expected %v, got %v", i, p, retPs[i]))
		}
	}

	return result
}
