package mongo

import (
	"reflect"
	"regexp"
	"testing"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
	"github.com/rs/rest-layer/schema/query"
	"gopkg.in/mgo.v2/bson"
)

type UnsupportedExpression struct{}

func (u UnsupportedExpression) Match(p map[string]interface{}) bool {
	return false
}

func (u UnsupportedExpression) Prepare(v schema.Validator) error {
	return nil
}

func (u UnsupportedExpression) String() string {
	return ""
}

func TestTranslatePredicateString(t *testing.T) {
	cases := []struct {
		predicate string
		want      bson.M
	}{
		{`{id:"foo"}`, bson.M{"_id": "foo"}},
		{`{f:"foo"}`, bson.M{"f": "foo"}},
		{`{f:"foo",g:"baz"}`, bson.M{"f": "foo", "g": "baz"}},
		{`{f:{$ne:"foo"}}`, bson.M{"f": bson.M{"$ne": "foo"}}},
		{`{f:{$exists:true}}`, bson.M{"f": bson.M{"$exists": true}}},
		{`{f:{$exists:false}}`, bson.M{"f": bson.M{"$exists": false}}},
		{`{f:{$gt:1}}`, bson.M{"f": bson.M{"$gt": float64(1)}}},
		{`{f:{$gte:1}}`, bson.M{"f": bson.M{"$gte": float64(1)}}},
		{`{f:{$lt:1}}`, bson.M{"f": bson.M{"$lt": float64(1)}}},
		{`{f:{$lte:1}}`, bson.M{"f": bson.M{"$lte": float64(1)}}},
		{`{f:{$in:["foo","bar"]}}`, bson.M{"f": bson.M{"$in": []interface{}{"foo", "bar"}}}},
		{`{f:{$nin:["foo","bar"]}}`, bson.M{"f": bson.M{"$nin": []interface{}{"foo", "bar"}}}},
		{`{f:{$regex:"fo[o]{1}.+is.+some"}}`, bson.M{"f": bson.M{"$regex": "fo[o]{1}.+is.+some"}}},
		{`{$and:[{f:"foo"},{f:"bar"}]}`, bson.M{"$and": []bson.M{{"f": "foo"}, {"f": "bar"}}}},
		{`{$or:[{f:"foo"},{f:"bar"}]}`, bson.M{"$or": []bson.M{{"f": "foo"}, {"f": "bar"}}}},
		{`{$or:[{f:"foo"},{f:"bar",g:"baz"}]}`, bson.M{"$or": []bson.M{{"f": "foo"}, {"$and": []bson.M{{"f": "bar"}, {"g": "baz"}}}}}},
		{`{f:{$elemMatch:{a:"foo",b:"bar"}}}`, bson.M{"f": bson.M{"$elemMatch": bson.M{"a": "foo", "b": "bar"}}}},
	}
	for i := range cases {
		tc := cases[i]
		t.Run(tc.predicate, func(t *testing.T) {
			got, err := translatePredicate(query.MustParsePredicate(tc.predicate))
			if err != nil {
				t.Errorf("translatePredicate error: %v", err)
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("translatePredicate:\ngot:  %#v\nwant: %#v", got, tc.want)
			}
		})
	}
}

func TestTranslatePredicate(t *testing.T) {
	cases := []struct {
		name      string
		predicate query.Predicate
		want      bson.M
	}{
		{
			name: "and expressions",
			predicate: query.Predicate{
				&query.And{
					&query.Equal{Field: "f", Value: "foo"},
					&query.And{
						&query.Equal{Field: "f", Value: "bar"},
						&query.Equal{Field: "g", Value: "baz"},
					},
				},
			},
			want: bson.M{
				"$and": []bson.M{
					{"f": "foo"},
					{"$and": []bson.M{{"f": "bar"}, {"g": "baz"}}},
				},
			},
		},
		{
			name: "and predicates",
			predicate: query.Predicate{
				&query.And{
					&query.Predicate{
						&query.Regex{Field: "f", Value: regexp.MustCompile("^b??$")},
					},
					&query.Predicate{
						&query.Equal{Field: "f", Value: "bar"},
						&query.Equal{Field: "g", Value: "baz"},
					},
				},
			},
			want: bson.M{
				"$and": []bson.M{
					{"f": bson.M{"$regex": "^b??$"}},
					{"f": "bar", "g": "baz"},
				},
			},
		},
		{
			name: "or expressions",
			predicate: query.Predicate{
				&query.Or{
					&query.Equal{Field: "f", Value: "foo"},
					&query.And{
						&query.Equal{Field: "f", Value: "bar"},
						&query.Equal{Field: "g", Value: "baz"},
					},
				},
			},
			want: bson.M{
				"$or": []bson.M{
					{"f": "foo"},
					{"$and": []bson.M{{"f": "bar"}, {"g": "baz"}}},
				},
			},
		},
		{
			name: "or predicates",
			predicate: query.Predicate{
				&query.Or{
					&query.Predicate{
						&query.Equal{Field: "f", Value: "foo"},
					},
					&query.Predicate{
						&query.Equal{Field: "f", Value: "bar"},
						&query.Equal{Field: "g", Value: "baz"},
					},
				},
			},
			want: bson.M{
				"$or": []bson.M{
					{"f": "foo"},
					{"f": "bar", "g": "baz"},
				},
			},
		},
	}
	for i := range cases {
		tc := cases[i]
		t.Run(tc.name, func(t *testing.T) {
			got, err := translatePredicate(tc.predicate)
			if err != nil {
				t.Errorf("translatePredicate error: %v", err)
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("translatePredicate:\ngot:  %#v\nwant: %#v", got, tc.want)
			}
		})
	}
}

func TestTranslatePredicateInvalid(t *testing.T) {
	var err error
	_, err = translatePredicate(query.Predicate{UnsupportedExpression{}})
	if resource.ErrNotImplemented != err {
		t.Errorf("expected ErrNotImplemented, got %v", err)
	}
	_, err = translatePredicate(query.Predicate{&query.And{UnsupportedExpression{}}})
	if resource.ErrNotImplemented != err {
		t.Errorf("expected ErrNotImplemented, got %v", err)
	}
	_, err = translatePredicate(query.Predicate{&query.Or{UnsupportedExpression{}}})
	if resource.ErrNotImplemented != err {
		t.Errorf("expected ErrNotImplemented, got %v", err)
	}
}

func TestGetSort(t *testing.T) {
	var s []string
	s = getSort(&query.Query{Sort: query.Sort{}})
	if expect := []string{"_id"}; !reflect.DeepEqual(expect, s) {
		t.Errorf("expected %v, got %v", expect, s)
	}
	s = getSort(&query.Query{Sort: query.Sort{{Name: "id"}}})
	if expect := []string{"_id"}; !reflect.DeepEqual(expect, s) {
		t.Errorf("expected %v, got %v", expect, s)
	}
	s = getSort(&query.Query{Sort: query.Sort{{Name: "f"}}})
	if expect := []string{"f"}; !reflect.DeepEqual(expect, s) {
		t.Errorf("expected %v, got %v", expect, s)
	}
	s = getSort(&query.Query{Sort: query.Sort{{Name: "f", Reversed: true}}})
	if expect := []string{"-f"}; !reflect.DeepEqual(expect, s) {
		t.Errorf("expected %v, got %v", expect, s)
	}
	s = getSort(&query.Query{Sort: query.Sort{{Name: "f"}, {Name: "f", Reversed: true}}})
	if expect := []string{"f", "-f"}; !reflect.DeepEqual(expect, s) {
		t.Errorf("expected %v, got %v", expect, s)
	}
}
