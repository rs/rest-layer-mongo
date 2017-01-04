package mongo

import (
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
	"gopkg.in/mgo.v2/bson"
)

// getField translate a schema field into a MongoDB field:
//
//  - id -> _id with in order to tape on the mongo primary key
func getField(f string) string {
	if f == "id" {
		return "_id"
	}
	return f
}

// getQuery transform a resource.Lookup into a Mongo query
func getQuery(l *resource.Lookup) (bson.M, error) {
	return translateQuery(l.Filter())
}

// getSort transform a resource.Lookup into a Mongo sort list.
// If the sort list is empty, fallback to _id.
func getSort(l *resource.Lookup) []string {
	ln := len(l.Sort())
	if ln == 0 {
		return []string{"_id"}
	}
	s := make([]string, ln)
	for i, sort := range l.Sort() {
		if len(sort) > 0 && sort[0] == '-' {
			s[i] = "-" + getField(sort[1:])
		} else {
			s[i] = getField(sort)
		}
	}
	return s
}

func translateQuery(q schema.Query) (bson.M, error) {
	b := bson.M{}
	for _, exp := range q {
		switch t := exp.(type) {
		case schema.And:
			s := []bson.M{}
			for _, subExp := range t {
				sb, err := translateQuery(schema.Query{subExp})
				if err != nil {
					return nil, err
				}
				s = append(s, sb)
			}
			b["$and"] = s
		case schema.Or:
			s := []bson.M{}
			for _, subExp := range t {
				sb, err := translateQuery(schema.Query{subExp})
				if err != nil {
					return nil, err
				}
				s = append(s, sb)
			}
			b["$or"] = s
		case schema.In:
			b[getField(t.Field)] = bson.M{"$in": valuesToInterface(t.Values)}
		case schema.NotIn:
			b[getField(t.Field)] = bson.M{"$nin": valuesToInterface(t.Values)}
		case schema.Equal:
			b[getField(t.Field)] = t.Value
		case schema.NotEqual:
			b[getField(t.Field)] = bson.M{"$ne": t.Value}
		case schema.GreaterThan:
			b[getField(t.Field)] = bson.M{"$gt": t.Value}
		case schema.GreaterOrEqual:
			b[getField(t.Field)] = bson.M{"$gte": t.Value}
		case schema.LowerThan:
			b[getField(t.Field)] = bson.M{"$lt": t.Value}
		case schema.LowerOrEqual:
			b[getField(t.Field)] = bson.M{"$lte": t.Value}
		case schema.Regex:
			b[getField(t.Field)] = bson.M{"$regex": t.Value.RegValue.String(), "$options": t.Value.RegOptions}
		default:
			return nil, resource.ErrNotImplemented
		}
	}
	return b, nil
}

func valuesToInterface(v []schema.Value) []interface{} {
	I := make([]interface{}, len(v))
	for i, _v := range v {
		I[i] = _v
	}
	return I
}
