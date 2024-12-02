package docx

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Request 代表一个资源请求实体
type Request struct {
	*Docx
	collection string         // mongo collection
	action     string         // 动作，find, delete, update, lock 等等
	id         string         // 资源 id
	filter     map[string]any // 资源过滤条件，用于 find
	sort       []string       // 排序，用于 find
	fields     []string       // 目标字段，用于 find 或 distinct
	group      []string
	ttl        int
	secret     string // secret 用于 lock，unlock，update
	page       int64  // 分页, 用于 find
	pageSize   int64  // 分页，一页大小 用于 find
	tag        string // 用于标签管理
	data       any    // 资源数据，用于 update 和 add
	dest       any    // dest 作为 decode response 的目标
}

func NewRequest(dst any) *Request {
	return Default.Request(dst)
}

// Reset 重置所有设定
func (r *Request) Reset() *Request {
	*r = Request{}
	return r
}

// Filter 设置 Find 过滤条件，只有在 Find 时有效
func (r *Request) Filter(key string, value any) *Request {
	r.filter[key] = value
	return r
}

// Filters 同 Filter，只不过可以一次性设置多个过滤条件
func (r *Request) Filters(filter map[string]any) *Request {
	for key, value := range filter {
		r.filter[key] = value
	}
	return r
}

// Page 设置 Find 时的分页，默认设置为 page = 1, size = 20
func (r *Request) Page(page, size int64) *Request {
	if page > 0 {
		r.page = page
	}
	if size > 0 {
		r.pageSize = size
	}
	return r
}

func (r *Request) Secret(secret string) *Request {
	r.secret = secret
	return r
}

// Sort 设置 Find 操作的排序方式，只对 Find 操作有效
func (r *Request) Sort(field string, asc bool) *Request {
	if asc {
		r.sort = append(r.sort, fmt.Sprintf("%s:1", field))
	} else {
		r.sort = append(r.sort, fmt.Sprintf("%s:-1", field))
	}
	return r
}

// Asc 设置 Find 操作的排序方式，只对 Find 操作有效
func (r *Request) Asc(fields ...string) *Request {
	for _, field := range fields {
		r.sort = append(r.sort, fmt.Sprintf("%s:1", field))
	}
	return r
}

// Desc 设置 Find 操作的排序方式，只对 Find 操作有效
func (r *Request) Desc(fields ...string) *Request {
	for _, field := range fields {
		r.sort = append(r.sort, fmt.Sprintf("%s:-1", field))
	}
	return r
}

// Fields 设置要查找的字段列表；对于一些内容比较臃肿的资源，指定 field 可以减少response体积。
func (r *Request) Fields(fields ...string) *Request {
	r.fields = fields
	return r
}

// DecodeTo 设置将 Response 中的数据解码到指定变量，通常为资源数据结构的指针，参考 json.Decoder.Decode 方法。
// 以 MyResource 数据结构为例，dest 通常为 *MyResource(Get) 或 *[]MyResource(Find)
func (r *Request) DecodeTo(dest any) *Request {
	r.dest = dest
	return r
}

// Find 查找资源对象列表
func (r *Request) Find() *Request {
	r.action = "find"
	return r
}

// Distinct 查找资源对象某个字段的枚举值
func (r *Request) Distinct(field string) *Request {
	r.action = "distinct"
	r.fields = []string{field}
	return r
}

// Count 统计资源对象数量
func (r *Request) Count(group string) *Request {
	r.action = "count"
	r.group = []string{group}
	return r
}

// Find 通过 Id 查找一个资源对象
func (r *Request) Get(id string) *Request {
	r.action = "get"
	r.id = id
	return r
}

// Delete 通过 Id 删除一个资源对象
func (r *Request) Delete(id string) *Request {
	r.action = "delete"
	r.id = id
	return r
}

// Add 添加一个资源对象
func (r *Request) Add(data any) *Request {
	r.action = "add"
	r.data = data
	return r
}

// Update 使用 Id 更新一个资源对象
func (r *Request) Update(id string, data any) *Request {
	r.action = "update"
	r.data = data
	r.id = id
	return r
}

// Update 使用 Id 更新一个资源对象
func (r *Request) AddTag(id string, tag string) *Request {
	r.action = "addTag"
	r.tag = tag
	r.id = id
	return r
}

// Update 使用 Id 更新一个资源对象
func (r *Request) RemoveTag(id string, tag string) *Request {
	r.action = "removeTag"
	r.tag = tag
	r.id = id
	return r
}

// Lock 使用 Id 锁住资源对象, 对锁住的资源对象无法更新(除了非敏感字段)和删除。
// 参数 secret 为锁的钥匙，只有使用该 secret 才能对资源进行 unlock。
func (r *Request) Lock(id string, secret string, ttl time.Duration) *Request {
	r.action = "lock"
	r.ttl = int(ttl / time.Second)
	r.secret = secret
	r.id = id
	return r
}

// Unlock 使用 Id 解锁资源对象，参数 secret 为 Lock 操作时锁使用的钥匙，否则会解锁失败。
func (r *Request) Unlock(id string, secret string) *Request {
	r.action = "unlock"
	r.ttl = -1
	r.secret = secret
	r.id = id
	return r
}

// validate 验证 Request 的合法性。
func (r *Request) validate() error {
	if r.collection == "" {
		return errResourceEmpty
	}

	switch r.action {
	case "":
		return errNeedAction
	case "get", "delete", "unlock":
		if r.id == "" {
			return fmt.Errorf("action %q: %w", r.action, errNeedId)
		}
	case "add":
		if r.data == nil {
			return fmt.Errorf("action %q: %w", r.action, errNeedData)
		}
	case "update":
		if r.id == "" {
			return fmt.Errorf("action %q: %w", r.action, errNeedId)
		} else if r.data == nil {
			return fmt.Errorf("action %q: %w", r.action, errNeedData)
		}
	case "lock":
		if r.id == "" {
			return fmt.Errorf("action %q: %w", r.action, errNeedId)
		}
		ttl, _ := time.ParseDuration(fmt.Sprintf("%s", r.filter["ttl"]))
		if ttl <= 0 {
			return fmt.Errorf("action %q: %w", r.action, errNeedTTL)
		}
	case "watch":
		destType := reflect.TypeOf(r.dest)
		if destType.Kind() != reflect.Chan || destType.Elem().Kind() != reflect.Slice {
			return fmt.Errorf("action %q: %w", r.action, errNeedSliceChanDest)
		}
	default:
		// TODO (@zhouhongyu.peterlits): 有一些没有被验证，个人还是认为使用子类代替会更
		// 好一些。
	}
	return nil
}

// Do 执行 Request，该方法会将先序设置的各种条件，生成一个 http 请求发送给 rser 服务。
func (r *Request) Do(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}

	switch r.action {
	case "delete":
		return r.delete(ctx)
	case "get":
		return r.get(ctx)
	case "find":
		return r.find(ctx)
	case "count":
		r.count(ctx)
	case "distinct":
		r.distinct(ctx)
	case "update":
		return r.update(ctx, 0, true)
	case "add":
		_, err := r.insert(ctx)
		return err
	case "lock":
		return r.lock(ctx, time.Now().Add(time.Second*time.Duration(r.ttl)))
	case "unlock":
		return r.lock(ctx, time.Now().Add(-time.Hour))
	case "addTag":
		return r.addTag(ctx)
	case "removeTag":
		return r.removeTag(ctx)
	}
	return nil
}

// insert preforms add a resource data into the scheme.
func (s *Request) insert(ctx context.Context) (Resource, error) {
	// Init the meta data and ID for resource data.
	version, err := s.getNewVersion(ctx)
	if err != nil {
		return Resource{}, fmt.Errorf("get version error: %w", Error(err))
	}
	doc := Resource{}
	if r, ok := s.data.(Resource); ok {
		doc = r
	}
	if doc.Id == "" {
		doc.Id = bson.NewObjectID().Hex()
	}
	doc.Meta = &Meta{
		Version:  version,
		Tags:     []string{},
		CreateAt: time.Now().Unix(),
		UpdateAt: time.Now().Unix(),
	}
	doc.Data = s.data

	// Insert the resource data into scheme collection.
	_, err = s.coll().InsertOne(ctx, doc)
	if err != nil {
		return doc, fmt.Errorf("insert error: %w", Error(err))
	}
	return doc, nil
}

// Update performs add a resource data into the scheme.
func (s *Request) update(ctx context.Context, prevVersion int64, upsert bool) error {
	if s.secret != "" {
		res := &Resource{}
		s.dest = res
		err := s.get(ctx)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil
			}
			return Error(err)
		}

		current := time.Now().Unix()
		if res.Meta.LockExpireAt >= current {
			if s.secret != res.Meta.LockSecret {
				return ErrResourceLocked
			}
		} else {
			log.Ctx(ctx).Debug().Str("id", s.id).
				Int64("current", current).Int64("data.Meta.LockExpireAt", res.Meta.LockExpireAt).
				Msg("Resource is not locked.")
		}
	}

	if prevVersion > 0 {
		s.filter[MetaKey+".version"] = prevVersion
	}

	filter := reviseFilter(s.filter)
	var updateData any
	switch d := s.data.(type) {
	case bson.M:
		updateData = reviseUpdateData(d, 0, 0)
	case map[string]any:
		updateData = reviseUpdateData(d, 0, 0)
	}
	log.Ctx(ctx).Info().Interface("filter", filter).Interface("data", updateData).Msg("Update the resource")
	_, err := s.coll().UpdateMany(ctx, filter, updateData, options.Update().SetUpsert(upsert))
	if err != nil {
		return fmt.Errorf("update many error: %w", err)
	}
	return nil
}

// Delete preforms delete a resource by its ID in the scheme.
func (s *Request) delete(ctx context.Context) error {
	log.Ctx(ctx).Debug().Str("id", s.id).Msg("Deleting resource by ID...")

	res := &Resource{}
	s.dest = res

	if s.secret != "" {
		err := s.get(ctx)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil
			}
			return Error(err)
		}

		current := time.Now().Unix()
		if res.Meta.LockExpireAt >= current {
			if s.secret != res.Meta.LockSecret {
				return ErrResourceLocked
			}
		} else {
			log.Ctx(ctx).Debug().Str("id", s.id).
				Int64("current", current).Int64("data.Meta.LockExpireAt", res.Meta.LockExpireAt).
				Msg("Resource is not locked.")
		}
	}

	// 删除 really.
	_, err := s.coll().DeleteOne(ctx, bson.M{IdKey: s.id})
	if err != nil {
		return Error(err)
	}

	// getNewVersion making the version +1
	_, err = s.getNewVersion(ctx)
	return Error(err)
}

// Find returns the list of resource.
func (s *Request) find(ctx context.Context) error {
	filter := reviseFilter(s.filter)
	sort := reviseFilter(parseSort(s.sort))
	skip := (s.page - 1) * s.pageSize
	if skip < 0 {
		skip = 0
	}

	opt := options.Find().
		SetSort(sort).SetLimit(s.pageSize).SetSkip(skip).SetProjection(projection(s.fields))
	log.Ctx(ctx).Info().
		Str("scheme", s.collection).
		Interface("filter", filter).Interface("sort", sort).Int64("limit", s.pageSize).Int64("skip", skip).
		Msg("Finding the resource")
	cursor, err := s.coll().Find(ctx, filter, opt)
	if err != nil {
		return Error(err)
	}
	if cursor.Err() != nil {
		return Error(cursor.Err())
	}
	defer cursor.Close(ctx)
	err = cursor.All(ctx, s.dest)
	log.Ctx(ctx).Debug().Interface("data", s.dest).Err(err).Interface("filter", filter).
		Msg("Finding response")
	return Error(err)
}

// Count TODO
func (s *Request) count(ctx context.Context) error {
	var pipeline []bson.M
	if len(s.filter) > 0 {
		filter := reviseFilter(s.filter)
		pipeline = append(pipeline, bson.M{"$match": filter})
	}
	if len(s.group) > 0 {
		pipeline = append(
			pipeline,
			bson.M{"$group": bson.M{"_id": "$" + DataKey + "." + s.group[0], "count": bson.M{"$sum": 1}}},
		)
	} else {
		pipeline = append(pipeline, bson.M{"$group": bson.M{"_id": nil, "count": bson.M{"$sum": 1}}})
	}

	log.Ctx(ctx).Info().
		Str("scheme", s.collection).Interface("pipeline", pipeline).
		Msg("Counting the resource.")
	cursor, err := s.coll().Aggregate(ctx, pipeline)
	if err != nil {
		return fmt.Errorf("aggregate error: %w", Error(err))
	}
	if err := cursor.All(ctx, s.dest); err != nil {
		return fmt.Errorf("cursor decode error: %w", err)
	}
	return Error(err)
}

// Distinct TODO
func (s *Request) distinct(ctx context.Context) error {
	if len(s.fields) == 0 {
		return fmt.Errorf("field required")
	}
	filter := reviseFilter(s.filter).(bson.M)
	field := s.fields[0]

	log.Ctx(ctx).Info().
		Str("collection", s.collection).Interface("filter", filter).Str("field", field).
		Msg("Distinct the resource")
	if !strings.HasPrefix(field, MetaKey+".") {
		field = DataKey + "." + field
	}
	result := s.coll().Distinct(ctx, field, filter)
	if err := result.Err(); err != nil {
		return Error(result.Err())
	}
	err := result.Decode(s.dest)
	return Error(err)
}

// Data returns resource data. And the fields of data are limited by
// argument fields.
//
// If the argument fields is nil, it will return the full data.
func (r *Request) get(ctx context.Context) error {
	log := log.Ctx(ctx).With().Str("id", r.id).Logger()
	var res Resource

	// Find the data.
	log.Debug().Msg("Finding one resource by ID...")
	findOneOption := options.FindOne().SetProjection(projection(r.fields))
	resMongoResult := r.coll().FindOne(ctx, bson.M{IdKey: r.id}, findOneOption)
	if err := resMongoResult.Err(); err != nil {
		log.Error().Err(err).Msg("Fail to find the resource by its ID.")
		return fmt.Errorf("find the resource by its ID: %w", Error(err))
	}

	// Decode to result.
	err := resMongoResult.Decode(r.dest)
	if res.Meta == nil {
		log.Error().Err(err).Msg("Fail to decode the resource into resource struct.")
		res.Meta = DefaultMeta()
	}
	if err != nil {
		return fmt.Errorf("decode: %w", Error(err))
	}
	return nil
}

// Lock preforms lock the resource until deadline.
func (r *Request) lock(ctx context.Context, deadline time.Time) error {
	log.Ctx(ctx).Info().Msg("(un)lock the resource")

	res := Resource{}
	r.dest = res
	// Find the resource from db.
	err := r.get(ctx)
	if err != nil {
		if deadline.Before(time.Now()) && err == mongo.ErrNoDocuments {
			// 对不存在资源解锁操作，返回成功。
			log.Ctx(ctx).Info().
				Msg("Cannot find document - return")
			return nil
		}
		return Error(err)
	}
	log.Ctx(ctx).Info().
		Str("given secret", r.secret).Str("original secret", res.Meta.LockSecret).
		Interface("deadline", deadline).
		Msg("(un)lock the resource")

	// Check the secret.
	if res.Meta.LockExpireAt >= time.Now().Unix() && res.Meta.LockSecret != r.secret {
		log.Ctx(ctx).Error().
			Str("given secret", r.secret).Str("original secret", res.Meta.LockSecret).
			Msg("Lock again with wrong secret.")
		return ErrResourceLocked
	}

	// Update the lock.
	filter := bson.M{
		IdKey: r.id,
		"$or": []bson.M{
			{
				MetaKey + "." + LockSecretKey: r.secret,
			},
			{
				MetaKey + "." + LockExpireKey: bson.M{
					"$lte": time.Now().Unix(),
				},
			},
		},
	}
	result, err := r.Docx.collection(r.collection).UpdateOne(ctx, filter, bson.M{
		"$set": bson.M{
			MetaKey + "." + LockExpireKey: deadline.Unix(),
			MetaKey + "." + LockSecretKey: r.secret,
		},
	})
	if err != nil {
		return Error(err)
	}
	if result.MatchedCount == 0 {
		return errors.New("lock failed")
	}
	return nil
}

// AddTag will add tag into the resource data by resource ID in the scheme.
func (r *Request) addTag(ctx context.Context) error {
	var res Resource
	r.dest = &res
	if err := r.get(ctx); err != nil {
		return err
	}

	for _, item := range res.Meta.Tags {
		if r.tag == item {
			return nil
		}
	}
	r.data = bson.M{MetaKey + "." + TagKey: append(res.Meta.Tags, r.tag)}
	return r.update(ctx, 0, false)
}

// Remove will remove tag from the resource data by resource ID in the scheme.
func (r *Request) removeTag(ctx context.Context) error {
	var res Resource
	r.dest = &res
	if err := r.get(ctx); err != nil {
		return err
	}
	for i := range res.Meta.Tags {
		if res.Meta.Tags[i] == r.tag {
			r.data = bson.M{MetaKey + "." + TagKey: append(res.Meta.Tags[:i], res.Meta.Tags[i+1:]...)}
			return r.update(ctx, res.Meta.Version, false)
		}
	}
	return nil
}

// coll will return the Collection value of the scheme s.
func (s *Request) coll() *mongo.Collection {
	return s.Docx.collection(s.collection)
}

// getNewVersion will get new version of the scheme (by find the old version,
// increase it and then return the new version)
func (s *Request) getNewVersion(ctx context.Context) (int64, error) {
	// Try to update the version and get the result.
	result := s.Docx.collection(versionCollection).FindOneAndUpdate(ctx, bson.M{"name": s.collection}, bson.M{
		"$inc": bson.M{"version": 1},
	}, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After))
	err := result.Err()
	if err == mongo.ErrNoDocuments {
		return 1, nil
	}
	if err != nil {
		return 0, fmt.Errorf("update version error: %w", Error(err))
	}

	// Decode and return.
	var data struct {
		Version int64 `bson:"version"`
	}
	if err := result.Decode(&data); err != nil {
		return 0, fmt.Errorf("decode version error: %w", Error(err))
	}
	return data.Version, nil
}

// reviseUpdateData 修订更新用数据。参数如下：
//   - `version`：元信息 `version` 应更新为的值。
//   - `createAt`：元信息 `createAt` 应更新为的值。
func reviseUpdateData(data bson.M, version int64, createAt int64) bson.M {
	dollar := map[string]interface{}{}
	for key := range data {
		// $ 开头的为 mongo 运算符，事先过滤掉。
		if strings.HasPrefix(key, "$") {
			dollar[key] = data[key]
			delete(data, key)
		} else if key == "meta" || key == "id" {
			// id 和 meta 是内置 key, 不允许被更新，直接忽略。
			delete(data, key)
		}
	}
	data = reviseData(data).(bson.M)

	// 设置元信息。
	if version > 0 {
		data[MetaKey+"."+VersionKey] = version
	}
	if createAt > 0 {
		data[MetaKey+"."+CreateAtKey] = createAt
	}
	data[MetaKey+"."+UpdateAtKey] = time.Now().Unix()

	update := bson.M{
		"$set": data,
	}

	// 把 $ 开头的运算符加回去。
	for key, value := range dollar {
		update[key] = reviseData(value)
	}
	return update
}

// 修订 Filter，其中返回值的类型和参数 `filter` 的类型一致。目的是将非关键字的 key （见函数
// `isKeyword`）加上 `resource.DataKey` 标识符。
//
// 示例（使用 JSON 的语法格式，实际上，令本函数支持 `filter` 类型为 `T`，那么 `T` 为类型
// `bson.M`、`bson.D`、`map[string]T`、`[]T` 之一）：
//
//	[{ "_id": "1", "name": "a", "meta.tags": "a.b.c" }]
//
// 假设 `resource.DataKey == "data"`，那么有：
//
//	[{ "_id": "1", "data.name": "a", "meta.tags": "a.b.c" }]
func reviseFilter(filter interface{}) interface{} {
	switch data := filter.(type) {
	case bson.M:
		next := bson.M{}
		for key, value := range data {
			if !isKeyword(key) {
				next[DataKey+"."+key] = reviseFilter(value)
			} else {
				next[key] = reviseFilter(value)
			}
		}
		return next
	case map[string]interface{}:
		next := map[string]interface{}{}
		for key, value := range data {
			if !isKeyword(key) {
				next[DataKey+"."+key] = reviseFilter(value)
			} else {
				next[key] = reviseFilter(value)
			}
		}
		return next
	case []interface{}:
		next := []interface{}{}
		for _, value := range data {
			next = append(next, reviseFilter(value))
		}
		return next
	case []map[string]interface{}:
		next := []map[string]interface{}{}
		for _, value := range data {
			next = append(next, reviseFilter(value).(map[string]interface{}))
		}
		return next
	case []bson.M:
		next := []bson.M{}
		for _, value := range data {
			next = append(next, reviseFilter(value).(bson.M))
		}
		return next
	case bson.D:
		next := bson.D{}
		for _, item := range data {
			item := item
			if isKeyword(item.Key) {
				next = append(next, item)
			} else {
				next = append(next, bson.E{Key: DataKey + "." + item.Key, Value: item.Value})
			}
		}
		return next
	}
	return filter
}

// reviseData 修订数据。
func reviseData(data interface{}) interface{} {
	switch data := data.(type) {
	case bson.M:
		next := bson.M{}
		for key, value := range data {
			if !isKeyword(key) {
				next[DataKey+"."+key] = value
			} else if strings.HasPrefix(key, "$") {
				next[key] = reviseData(value)
			} else {
				next[key] = value
			}
		}
		return next
	case map[string]interface{}:
		next := map[string]interface{}{}
		for key, value := range data {
			if !isKeyword(key) {
				next[DataKey+"."+key] = value
			} else if strings.HasPrefix(key, "$") {
				next[key] = reviseData(value)
			} else {
				next[key] = value
			}
		}
		return next
	case []interface{}:
		next := []interface{}{}
		for _, value := range data {
			next = append(next, reviseData(value))
		}
		return next
	case []map[string]interface{}:
		next := []map[string]interface{}{}
		for _, value := range data {
			next = append(next, reviseData(value).(map[string]interface{}))
		}
		return next
	case []bson.M:
		next := []bson.M{}
		for _, value := range data {
			next = append(next, reviseData(value).(bson.M))
		}
		return next
	case bson.D:
		next := bson.D{}
		for _, item := range data {
			item := item
			if !isKeyword(item.Key) {
				next = append(next, bson.E{Key: DataKey + "." + item.Key, Value: item.Value})
			} else if strings.HasPrefix(item.Key, "$") {
				next = append(next, bson.E{Key: item.Key, Value: reviseData(item.Value)})
			} else {
				next = append(next, item)
			}
		}
		return next
	}
	return data
}

// isKeyword 判断特殊含义的 key：
//   - 以美元符号 `$` 开头。
//   - `_id`：这标识了 MongoDB 文档的唯一标识符。
//   - 以 `meta.` 开头，表示是文档的元信息。
func isKeyword(key string) bool {
	return strings.HasPrefix(key, "$") || key == "_id" || strings.HasPrefix(key, "meta.")
}

func isBuiltinIndex(name string) bool {
	if name == idIndexName {
		return true
	}
	for _, index := range builtinIndexes {
		if name == index.Name {
			return true
		}
	}
	return false
}

func projection(fields []string) bson.M {
	if len(fields) == 0 {
		return nil
	}
	m := make(bson.M)
	for _, f := range fields {
		m["data."+f] = 1
	}
	m["_id"] = 1
	m["meta"] = 1
	return m
}

// parseSort returns the sort from ctx.
func parseSort(data []string) bson.D {
	sort := bson.D{}
	for _, x := range data {
		item := string(x)
		if item == "" {
			continue
		}
		if strings.HasSuffix(item, ":-1") {
			sort = append(sort, bson.E{Key: item[:len(item)-3], Value: -1})
		} else if strings.HasSuffix(item, ":1") {
			sort = append(sort, bson.E{Key: item[:len(item)-2], Value: 1})
		} else {
			sort = append(sort, bson.E{Key: item, Value: 1})
		}
	}
	return sort
}
