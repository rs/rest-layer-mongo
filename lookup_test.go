package mongo

import (
	"testing"

	"regexp"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
	"github.com/rs/rest-layer/schema/query"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
)

type UnsupportedExpression struct{}

func (u UnsupportedExpression) Match(p map[string]interface{}) bool {
	return false
}

func (u UnsupportedExpression) Validate(v schema.Validator) error {
	return nil
}

func (u UnsupportedExpression) String() string {
	return ""
}

func callGetQuery(q query.Query) (bson.M, error) {
	l := resource.NewLookup()
	l.AddQuery(q)
	return getQuery(l)
}

func callGetSort(s string, v schema.Validator) []string {
	l := resource.NewLookup()
	l.SetSort(s, v)
	return getSort(l)
}

func TestGetQuery(t *testing.T) {
	var b bson.M
	var err error
	b, err = callGetQuery(query.Query{query.Equal{Field: "id", Value: "foo"}})
	assert.NoError(t, err)
	assert.Equal(t, bson.M{"_id": "foo"}, b)
	b, err = callGetQuery(query.Query{query.Equal{Field: "f", Value: "foo"}})
	assert.NoError(t, err)
	assert.Equal(t, bson.M{"f": "foo"}, b)
	b, err = callGetQuery(query.Query{query.NotEqual{Field: "f", Value: "foo"}})
	assert.NoError(t, err)
	assert.Equal(t, bson.M{"f": bson.M{"$ne": "foo"}}, b)
	b, err = callGetQuery(query.Query{query.GreaterThan{Field: "f", Value: 1}})
	assert.NoError(t, err)
	assert.Equal(t, bson.M{"f": bson.M{"$gt": float64(1)}}, b)
	b, err = callGetQuery(query.Query{query.GreaterOrEqual{Field: "f", Value: 1}})
	assert.NoError(t, err)
	assert.Equal(t, bson.M{"f": bson.M{"$gte": float64(1)}}, b)
	b, err = callGetQuery(query.Query{query.LowerThan{Field: "f", Value: 1}})
	assert.NoError(t, err)
	assert.Equal(t, bson.M{"f": bson.M{"$lt": float64(1)}}, b)
	b, err = callGetQuery(query.Query{query.LowerOrEqual{Field: "f", Value: 1}})
	assert.NoError(t, err)
	assert.Equal(t, bson.M{"f": bson.M{"$lte": float64(1)}}, b)
	b, err = callGetQuery(query.Query{query.In{Field: "f", Values: []query.Value{"foo", "bar"}}})
	assert.NoError(t, err)
	assert.Equal(t, bson.M{"f": bson.M{"$in": []interface{}{"foo", "bar"}}}, b)
	b, err = callGetQuery(query.Query{query.NotIn{Field: "f", Values: []query.Value{"foo", "bar"}}})
	assert.NoError(t, err)
	assert.Equal(t, bson.M{"f": bson.M{"$nin": []interface{}{"foo", "bar"}}}, b)
	if v, err := regexp.Compile("fo[o]{1}.+is.+some"); err == nil {
		b, err = callGetQuery(query.Query{query.Regex{Field: "f", Value: v}})
	}
	assert.NoError(t, err)
	assert.Equal(t, bson.M{"f": bson.M{"$regex": "fo[o]{1}.+is.+some"}}, b)
	b, err = callGetQuery(query.Query{query.And{query.Equal{Field: "f", Value: "foo"}, query.Equal{Field: "f", Value: "bar"}}})
	assert.NoError(t, err)
	assert.Equal(t, bson.M{"$and": []bson.M{bson.M{"f": "foo"}, bson.M{"f": "bar"}}}, b)
	b, err = callGetQuery(query.Query{query.Or{query.Equal{Field: "f", Value: "foo"}, query.Equal{Field: "f", Value: "bar"}}})
	assert.NoError(t, err)
	assert.Equal(t, bson.M{"$or": []bson.M{bson.M{"f": "foo"}, bson.M{"f": "bar"}}}, b)
}

func TestGetQueryInvalid(t *testing.T) {
	var err error
	_, err = callGetQuery(query.Query{UnsupportedExpression{}})
	assert.Equal(t, resource.ErrNotImplemented, err)
	_, err = callGetQuery(query.Query{query.And{UnsupportedExpression{}}})
	assert.Equal(t, resource.ErrNotImplemented, err)
	_, err = callGetQuery(query.Query{query.Or{UnsupportedExpression{}}})
	assert.Equal(t, resource.ErrNotImplemented, err)
}

func TestGetSort(t *testing.T) {
	var s []string
	v := schema.Schema{Fields: schema.Fields{"id": schema.IDField, "f": {Sortable: true}}}
	s = callGetSort("", v)
	assert.Equal(t, []string{"_id"}, s)
	s = callGetSort("id", v)
	assert.Equal(t, []string{"_id"}, s)
	s = callGetSort("f", v)
	assert.Equal(t, []string{"f"}, s)
	s = callGetSort("-f", v)
	assert.Equal(t, []string{"-f"}, s)
	s = callGetSort("f,-f", v)
	assert.Equal(t, []string{"f", "-f"}, s)
}
