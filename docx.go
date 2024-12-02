package docx

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	versionCollection      = "docx_version"
	idIndexName            = "_id_"
	historyMetaIdIndexName = "idx_meta_id"
)

var (
	builtinIndexes = []Index{
		{
			Name:       "idx_meta_createAt",
			Attributes: []string{MetaKey + "." + CreateAtKey},
			Unique:     false,
		},
	}

	Default *Docx
)

func Init(client *mongo.Client, database string) {
	Default = New(client, database)
}

type Docx struct {
	client *mongo.Client

	database string
}

func New(client *mongo.Client, database string) *Docx {
	x := Docx{
		client:   client,
		database: database,
	}
	return &x
}

// RegisterScheme will register a scheme into MongoDB by its Name (If there is
// an older scheme has the same Name, it will replace it).
func (docx *Docx) Bind(ctx context.Context, m any) error {

	var (
		collection = collectionFromModel(m)
		indexes    map[string]Index
		ttl        int32 = 0
	)

	// 设置索引
	if indexer, ok := m.(interface {
		Indexes() map[string]Index
	}); ok {
		indexes = indexer.Indexes()
	}

	// 是否过期
	re, ok := m.(interface {
		Expire() time.Duration
	})
	if ok {
		ttl = int32(re.Expire() / time.Second)
	}

	sparse := true
	coll := docx.collection(collection)

	// Drop the indexes that are in the database but not in the field
	// scheme.Indexes.
	oldIndexNameSet, err := docx.indexNameSet(ctx, collection)
	if err != nil {
		return nil
	}
	for name := range oldIndexNameSet {
		if isBuiltinIndex(name) {
			continue
		}
		if _, ok := indexes[name]; !ok {
			log.Ctx(ctx).Info().Str("index", name).Str("collection", collection).Msg("drop the index")
			if err := coll.Indexes().DropOne(ctx, name); err != nil {
				return Error(err)
			}
		}
	}

	var indexModels []mongo.IndexModel

	// Add the indexes that are not in the database but in the scheme.Indexes
	// into variable indexModels.
	for name, index := range indexes {
		name, index := name, index
		if _, ok := oldIndexNameSet[name]; ok {
			continue
		}
		keys := make(bson.D, 0, 8)
		for _, key := range index.Attributes {
			keys = append(keys, bson.E{Key: DataKey + "." + key, Value: 1})
		}

		log.Ctx(ctx).Info().
			Str("collection", collection).Str("index", name).Bool("unique", index.Unique).Interface("keys", keys).
			Msg("Create index.")
		indexModels = append(indexModels,
			mongo.IndexModel{
				Keys:    keys,
				Options: options.Index().SetName(name).SetUnique(index.Unique).SetSparse(sparse),
			},
		)
	}

	// Add the buildin indexes that are not in the database into variable
	// indexModels.
	for _, builtin := range builtinIndexes {
		builtin := builtin
		if _, ok := oldIndexNameSet[builtin.Name]; ok {
			continue
		}
		keys := bson.D{}
		for _, attr := range builtin.Attributes {
			keys = append(keys, bson.E{Key: attr, Value: 1})
		}
		iopts := options.Index().SetName(builtin.Name).SetUnique(builtin.Unique).SetSparse(sparse)
		if ttl > 0 {
			iopts = iopts.SetExpireAfterSeconds(ttl)
		}
		log.Ctx(ctx).Info().
			Str("collection", collection).Str("index", builtin.Name).Interface("keys", keys).
			Msg("Create builtin index")
		indexModels = append(indexModels, mongo.IndexModel{Keys: keys, Options: iopts})
	}

	// Create the indexes that are in the varable indexModels.
	if len(indexModels) > 0 {
		_, err = coll.Indexes().CreateMany(context.Background(), indexModels)
		if err != nil {
			return Error(err)
		}
	}

	return nil
}

// Unregister will drop the scheme.
func (docx *Docx) Unbind(ctx context.Context, m any) error {
	name := collectionFromModel(m)
	log.Ctx(ctx).Info().Str("collection", name).Msg("Drop collection")
	if err := docx.collection(name).Drop(ctx); err != nil {
		return Error(err)
	}
	return nil
}

func (docx *Docx) Request(dst any) *Request {
	r := &Request{
		Docx:       docx,
		collection: collectionFromModel(dst),
	}
	return r
}

// indexNameSet will returns the set of scheme index names of scheme.
func (docx *Docx) indexNameSet(ctx context.Context, collection string) (map[string]struct{}, error) {
	indexes := make(map[string]struct{})

	models, err := docx.collection(collection).Indexes().ListSpecifications(ctx)
	if err != nil {
		return nil, err
	}

	for _, model := range models {
		indexes[model.Name] = struct{}{}
	}
	return indexes, nil
}

// coll will return the Collection value of the scheme s.
func (docx *Docx) collection(name string) *mongo.Collection {
	return docx.client.Database(docx.database).Collection(name)
}

func collectionFromModel(d any) string {
	if s, ok := d.(string); ok {
		return s
	}

	var t reflect.Type
	for t = reflect.TypeOf(d); t.Kind() == reflect.Ptr || t.Kind() == reflect.Array || t.Kind() == reflect.Slice; {
		t = t.Elem()
	}
	if c, ok := t.(interface {
		Collection() string
	}); ok {
		return c.Collection()
	}

	return strings.ToLower(t.Name())
}

func Bind(ctx context.Context, m any) error {
	return Default.Bind(ctx, m)
}

func Unbind(ctx context.Context, m any) error {
	return Default.Unbind(ctx, m)
}
