package mongo_test

import (
	"regexp"
	"testing"

	"gopkg.in/mgo.v2/bson"

	"github.com/stretchr/testify/assert"

	mongo "github.com/rs/rest-layer-mongo"
)

const (
	validObjectID   = "59a40602952dbd0001c3ffc9"
	invalidObjectID = "59a40602952dbd0001c3ffc9f"
)

func TestObjectIDValidate(t *testing.T) {

	v := &mongo.ObjectID{}

	t.Run("validObjectID", func(t *testing.T) {
		id, err := v.Validate(validObjectID)
		assert.Equal(t, bson.ObjectIdHex(validObjectID), id, "v.Validate(validObjectID)")
		assert.NoError(t, err, "v.Validate(validObjectID)")
	})

	t.Run("invalidObjectID", func(t *testing.T) {
		id, err := v.Validate(invalidObjectID)
		assert.Nil(t, id, "v.Validate(invalidObjectID)")
		assert.Error(t, err, "v.Validate(invalidObjectID)")
	})
}

func TestObjectIDJSONSchmea(t *testing.T) {

	v := &mongo.ObjectID{}
	m, err := v.BuildJSONSchema()
	assert.NoError(t, err)
	assert.Equal(t, m["type"], "string")
	re, err := regexp.Compile(m["pattern"].(string))
	assert.NoError(t, err)

	t.Run("validObjectID", func(t *testing.T) {
		assert.True(t, re.MatchString(validObjectID), "re.Match(validObjectID)")
	})

	t.Run("invalidObjectID", func(t *testing.T) {
		assert.False(t, re.MatchString(invalidObjectID), "re.Match(invalidObjectID)")
	})
}
