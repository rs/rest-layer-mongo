package mongo

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/rest-layer/schema"
	"gopkg.in/mgo.v2/bson"
)

var (
	// NewObjectID is a field hook handler that generates a new Mongo ObjectID hex if
	// value is nil to be used in schema with OnInit.
	NewObjectID = func(ctx context.Context, value interface{}) interface{} {
		if value == nil {
			value = bson.NewObjectId().Hex()
		}
		return value
	}

	// ObjectIDField is a common schema field configuration that generate an Object ID
	// for new item id.
	ObjectIDField = schema.Field{
		Required:   true,
		ReadOnly:   true,
		OnInit:     NewObjectID,
		Filterable: true,
		Sortable:   true,
		Validator:  &ObjectID{},
	}
)

// ObjectID validates and serialize unique id
type ObjectID struct{}

// Validate implements FieldValidator interface
func (v ObjectID) Validate(value interface{}) (interface{}, error) {
	s, ok := value.(string)
	if !ok {
		return nil, errors.New("invalid object id")
	}
	if len(s) != 24 {
		return nil, errors.New("invalid object id length")
	}
	if !bson.IsObjectIdHex(s) {
		return nil, fmt.Errorf("invalid object id")
	}
	return bson.ObjectIdHex(s), nil
}

// Serialize implements FieldSerializer interface
func (v ObjectID) Serialize(value interface{}) (interface{}, error) {
	id, ok := value.(bson.ObjectId)
	if !ok {
		return nil, errors.New("not an ObjectId")
	}
	return id.Hex(), nil
}
