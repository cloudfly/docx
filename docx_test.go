package docx

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	MONGO_URI_ENV  = "MONGO_URI"
	MONGO_DATABASE = "docx_unit_test"
)

type TestPerson struct {
	Name     string    `bson:"name"`
	Age      int32     `bson:"age"`
	Birthday time.Time `bson:"birthday"`
	Health   bool      `bson:"health"`
	Habbits  []string  `bson:"habbits"`
}

func (person TestPerson) Indexes() map[string]Index {
	return map[string]Index{
		"idx_name": {
			Attributes: []string{"name"},
			Unique:     true,
		},
	}
}

func getTestMongoUri() string {
	uri := os.Getenv(MONGO_URI_ENV)
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}
	if !strings.HasPrefix(uri, "mongodb://") {
		uri = "mongodb://" + uri
	}
	return uri
}

func testBind(ctx context.Context, assert *require.Assertions) {
	testCollection := collectionFromModel(TestPerson{})
	client, err := mongo.Connect(options.Client().ApplyURI(getTestMongoUri()))
	assert.NoError(err)

	Init(client, MONGO_DATABASE)

	assert.NoError(Bind(ctx, TestPerson{}))

	collectionNames, err := Default.client.Database(MONGO_DATABASE).ListCollectionNames(ctx, bson.D{})
	assert.NoError(err)
	assert.Contains(collectionNames, testCollection, "The `testperson` should exist in collections %s", collectionNames)
}

func testUnbind(ctx context.Context, assert *require.Assertions) {
	testCollection := collectionFromModel(TestPerson{})
	assert.NoError(Unbind(ctx, TestPerson{}))
	collectionNames, err := Default.client.Database(MONGO_DATABASE).ListCollectionNames(ctx, bson.D{})
	assert.NoError(err)
	assert.NotContains(collectionNames, testCollection, "The `testperson` should not exist in collections %s", collectionNames)
}

func TestDocx(t *testing.T) {

	t.Run("Basic", func(t *testing.T) {
		var (
			assert         = require.New(t)
			ctx            = context.Background()
			testCollection = collectionFromModel(TestPerson{})
		)

		testBind(ctx, assert)
		defer testUnbind(ctx, assert)

		indexes := Default.client.Database(MONGO_DATABASE).Collection(testCollection).Indexes()
		cursor, err := indexes.List(ctx)
		assert.NoError(err)
		var idxData []struct {
			Name   string         `bson:"name"`
			Key    map[string]int `bson:"key"`
			Unique bool           `bson:"unique"`
			Sparse bool           `bson:"sparse"`
		}
		assert.NoError(cursor.All(ctx, &idxData))
		assert.Equal(len(idxData), 3)

		for _, idx := range idxData {
			switch idx.Name {
			case idIndexName:
				assert.Contains(idx.Key, "_id")
			case createAtIndexName:
				assert.Contains(idx.Key, "meta.createAt")
				assert.False(idx.Unique)
				assert.True(idx.Sparse)
			case "idx_name":
				assert.Contains(idx.Key, "data.name")
				assert.True(idx.Unique)
				assert.True(idx.Sparse)
			default:
				assert.Fail("unknown index name '%s'", idx.Name)
			}
		}

	})
}
