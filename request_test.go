package docx

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	testPerson = TestPerson{
		Name:     "John",
		Age:      32,
		Birthday: time.Now().Add(-time.Hour * 24 * 365 * 32),
		Health:   true,
		Habbits:  []string{"food", "bike"},
	}
)

func TestRequest(t *testing.T) {
	var (
		assert = require.New(t)
		ctx    = context.Background()
		//		testCollection = collectionFromModel(TestPerson{})
	)
	testBind(ctx, assert)
	defer testUnbind(ctx, assert)

	assert.NoError(NewRequest(TestPerson{}).Add(testPerson).Do(ctx))

	persons := []TestPerson{}
	assert.NoError(NewRequest(TestPerson{}).Find().DecodeTo(&persons).Do(ctx))
	t.Log(persons)
}
