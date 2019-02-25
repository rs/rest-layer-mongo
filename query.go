package mongo

import (
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema/query"
	mgo "gopkg.in/mgo.v2"
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

// getQuery transform a query into a Mongo query.
func getQuery(q *query.Query) (bson.M, error) {
	return translatePredicate(q.Predicate)
}

// getSort transform a resource.Lookup into a Mongo sort list.
// If the sort list is empty, fallback to _id.
func getSort(q *query.Query) []string {
	if len(q.Sort) == 0 {
		return []string{"_id"}
	}
	s := make([]string, len(q.Sort))
	for i, sort := range q.Sort {
		if sort.Reversed {
			s[i] = "-" + getField(sort.Name)
		} else {
			s[i] = getField(sort.Name)
		}
	}
	return s
}

func applyWindow(mq *mgo.Query, w query.Window) *mgo.Query {
	if w.Offset > 0 {
		mq = mq.Skip(w.Offset)
	}
	if w.Limit > -1 {
		mq = mq.Limit(w.Limit)
	}
	return mq
}

func selectIDs(c *mgo.Collection, mq *mgo.Query) ([]interface{}, error) {
	var ids []interface{}
	tmp := struct {
		ID interface{} `bson:"_id"`
	}{}
	it := mq.Select(bson.M{"_id": 1}).Iter()
	for it.Next(&tmp) {
		ids = append(ids, tmp.ID)
	}
	if err := it.Close(); err != nil {
		return nil, err
	}
	return ids, nil
}

func translatePredicate(q query.Predicate) (bson.M, error) {
	b := bson.M{}
	for _, exp := range q {
		switch t := exp.(type) {
		case *query.And:
			s := []bson.M{}
			for _, subExp := range *t {
				sb, err := translatePredicate(query.Predicate{subExp})
				if err != nil {
					return nil, err
				}
				s = append(s, sb)
			}
			b["$and"] = s
		case *query.Or:
			s := []bson.M{}
			for _, subExp := range *t {
				sb, err := translatePredicate(query.Predicate{subExp})
				if err != nil {
					return nil, err
				}
				s = append(s, sb)
			}
			b["$or"] = s
		case *query.ElemMatch:
			s := bson.M{}
			for _, subExp := range t.Exps {
				sb, err := translatePredicate(query.Predicate{subExp})
				if err != nil {
					return nil, err
				}
				for k, v := range sb {
					s[k] = v
				}
			}
			b[getField(t.Field)] = bson.M{"$elemMatch": s}
		case *query.In:
			b[getField(t.Field)] = bson.M{"$in": t.Values}
		case *query.NotIn:
			b[getField(t.Field)] = bson.M{"$nin": t.Values}
		case *query.Exist:
			b[getField(t.Field)] = bson.M{"$exists": true}
		case *query.NotExist:
			b[getField(t.Field)] = bson.M{"$exists": false}
		case *query.Equal:
			b[getField(t.Field)] = t.Value
		case *query.NotEqual:
			b[getField(t.Field)] = bson.M{"$ne": t.Value}
		case *query.GreaterThan:
			b[getField(t.Field)] = bson.M{"$gt": t.Value}
		case *query.GreaterOrEqual:
			b[getField(t.Field)] = bson.M{"$gte": t.Value}
		case *query.LowerThan:
			b[getField(t.Field)] = bson.M{"$lt": t.Value}
		case *query.LowerOrEqual:
			b[getField(t.Field)] = bson.M{"$lte": t.Value}
		case *query.Regex:
			b[getField(t.Field)] = bson.M{"$regex": t.Value.String()}
		default:
			return nil, resource.ErrNotImplemented
		}
	}
	return b, nil
}
