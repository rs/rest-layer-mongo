package mongo

import (
	"context"
	"testing"
	"time"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2"
)

// Mongo doesn't support nanoseconds
var now = time.Now().Round(time.Millisecond)

// cleanup deletes a database immediately and on defer when call as:
//
//   defer cleanup(c, "database")()
func cleanup(s *mgo.Session, db string) func() {
	s.DB(db).DropDatabase()
	return func() {
		s.DB(db).DropDatabase()
	}
}

func TestInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	s, err := mgo.Dial("")
	if !assert.NoError(t, err) {
		return
	}
	defer cleanup(s, "testinsert")()
	h := NewHandler(s, "testinsert", "test")
	items := []*resource.Item{
		{
			ID:      "1234",
			ETag:    "etag",
			Updated: now,
			Payload: map[string]interface{}{
				"id":  "1234",
				"foo": "bar",
			},
		},
	}
	err = h.Insert(context.Background(), items)
	assert.NoError(t, err)
	d := map[string]interface{}{}
	err = s.DB("testinsert").C("test").FindId("1234").One(&d)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, map[string]interface{}{"foo": "bar", "_id": "1234", "_etag": "etag", "_updated": now}, d)

	// Inserting same item twice should return a conflict error
	err = h.Insert(context.Background(), items)
	assert.Equal(t, resource.ErrConflict, err)
}

func TestUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	s, err := mgo.Dial("")
	if !assert.NoError(t, err) {
		return
	}
	defer cleanup(s, "testupdate")()
	h := NewHandler(s, "testupdate", "test")
	oldItem := &resource.Item{
		ID:      "1234",
		ETag:    "etag1",
		Updated: now,
		Payload: map[string]interface{}{
			"id":  "1234",
			"foo": "bar",
		},
	}
	newItem := &resource.Item{
		ID:      "1234",
		ETag:    "etag2",
		Updated: now,
		Payload: map[string]interface{}{
			"id":  "1234",
			"foo": "baz",
		},
	}

	// Can't update a non existing item
	err = h.Update(context.Background(), newItem, oldItem)
	assert.Equal(t, resource.ErrNotFound, err)

	err = h.Insert(context.Background(), []*resource.Item{oldItem})
	assert.NoError(t, err)
	err = h.Update(context.Background(), newItem, oldItem)
	assert.NoError(t, err)

	// Update refused if original item's etag doesn't match stored one
	err = h.Update(context.Background(), newItem, oldItem)
	assert.Equal(t, resource.ErrConflict, err)
}

func TestDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	s, err := mgo.Dial("")
	if !assert.NoError(t, err) {
		return
	}
	defer cleanup(s, "testupdate")()
	h := NewHandler(s, "testupdate", "test")
	item := &resource.Item{
		ID:      "1234",
		ETag:    "etag1",
		Updated: now,
		Payload: map[string]interface{}{
			"id":  "1234",
			"foo": "bar",
		},
	}

	// Can't delete a non existing item
	err = h.Delete(context.Background(), item)
	assert.Equal(t, resource.ErrNotFound, err)

	err = h.Insert(context.Background(), []*resource.Item{item})
	assert.NoError(t, err)
	err = h.Delete(context.Background(), item)
	assert.NoError(t, err)

	// Update refused if original item's etag doesn't match stored one
	err = h.Insert(context.Background(), []*resource.Item{item})
	assert.NoError(t, err)
	item.ETag = "etag2"
	err = h.Delete(context.Background(), item)
	assert.Equal(t, resource.ErrConflict, err)
}

func TestClear(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	s, err := mgo.Dial("")
	if !assert.NoError(t, err) {
		return
	}
	defer cleanup(s, "testupdate")()
	h := NewHandler(s, "testupdate", "test")
	items := []*resource.Item{
		{ID: "1", Payload: map[string]interface{}{"id": "1", "name": "a"}},
		{ID: "2", Payload: map[string]interface{}{"id": "2", "name": "b"}},
		{ID: "3", Payload: map[string]interface{}{"id": "3", "name": "c"}},
		{ID: "4", Payload: map[string]interface{}{"id": "4", "name": "d"}},
	}

	err = h.Insert(context.Background(), items)
	assert.NoError(t, err)

	lookup := resource.NewLookupWithQuery(schema.Query{
		schema.In{Field: "name", Values: []schema.Value{"c", "d"}},
	})
	deleted, err := h.Clear(context.Background(), lookup)
	assert.NoError(t, err)
	assert.Equal(t, 2, deleted)

	lookup = resource.NewLookupWithQuery(schema.Query{
		schema.Equal{Field: "id", Value: "2"},
	})
	deleted, err = h.Clear(context.Background(), lookup)
	assert.NoError(t, err)
	assert.Equal(t, 1, deleted)
}

func TestFind(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	s, err := mgo.Dial("")
	if !assert.NoError(t, err) {
		return
	}
	defer cleanup(s, "testfind")()
	h := NewHandler(s, "testfind", "test")
	h2 := NewHandler(s, "testfind", "test2")
	items := []*resource.Item{
		{ID: "1", Payload: map[string]interface{}{"id": "1", "name": "a", "age": 1}},
		{ID: "2", Payload: map[string]interface{}{"id": "2", "name": "b", "age": 2}},
		{ID: "3", Payload: map[string]interface{}{"id": "3", "name": "c", "age": 3}},
		{ID: "4", Payload: map[string]interface{}{"id": "4", "name": "d", "age": 4}},
	}
	ctx := context.Background()
	assert.NoError(t, h.Insert(ctx, items))
	assert.NoError(t, h2.Insert(ctx, items))

	lookup := resource.NewLookup()
	l, err := h.Find(ctx, lookup, 0, -1)
	if assert.NoError(t, err) {
		assert.Equal(t, -1, l.Total) // Mongo doesn't support total counting
		assert.Len(t, l.Items, 4)
		// Do not check result's content as its order is unpredictable
	}

	lookup = resource.NewLookupWithQuery(schema.Query{
		schema.Equal{Field: "name", Value: "c"},
	})
	l, err = h.Find(ctx, lookup, 0, 100)
	if assert.NoError(t, err) {
		assert.Equal(t, -1, l.Total) // Mongo doesn't support total counting
		if assert.Len(t, l.Items, 1) {
			item := l.Items[0]
			assert.Equal(t, "3", item.ID)
			assert.Equal(t, map[string]interface{}{"id": "3", "name": "c", "age": 3}, item.Payload)
		}
	}

	lookup = resource.NewLookupWithQuery(schema.Query{
		schema.In{Field: "name", Values: []schema.Value{"c", "d"}},
	})
	lookup.SetSorts([]string{"name"})
	l, err = h.Find(ctx, lookup, 0, 100)
	if assert.NoError(t, err) {
		assert.Equal(t, -1, l.Total) // Mongo doesn't support total counting
		if assert.Len(t, l.Items, 2) {
			item := l.Items[0]
			assert.Equal(t, "3", item.ID)
			assert.Equal(t, map[string]interface{}{"id": "3", "name": "c", "age": 3}, item.Payload)
			item = l.Items[1]
			assert.Equal(t, "4", item.ID)
			assert.Equal(t, map[string]interface{}{"id": "4", "name": "d", "age": 4}, item.Payload)
		}
	}

	lookup = resource.NewLookupWithQuery(schema.Query{
		schema.Equal{Field: "id", Value: "3"},
	})
	l, err = h.Find(ctx, lookup, 0, 1)
	if assert.NoError(t, err) {
		assert.Equal(t, -1, l.Total) // Mongo doesn't support total counting
		if assert.Len(t, l.Items, 1) {
			item := l.Items[0]
			assert.Equal(t, "3", item.ID)
			assert.Equal(t, map[string]interface{}{"id": "3", "name": "c", "age": 3}, item.Payload)
		}
	}

	lookup = resource.NewLookupWithQuery(schema.Query{
		schema.Equal{Field: "id", Value: "10"},
	})
	l, err = h.Find(ctx, lookup, 0, 1)
	if assert.NoError(t, err) {
		assert.Equal(t, -1, l.Total) // Mongo doesn't support total counting
		assert.Len(t, l.Items, 0)
	}

	lookup = resource.NewLookupWithQuery(schema.Query{
		schema.In{Field: "id", Values: []schema.Value{"3", "4", "10"}},
	})
	l, err = h.Find(ctx, lookup, 0, -1)
	if assert.NoError(t, err) {
		assert.Equal(t, -1, l.Total) // Mongo doesn't support total counting
		assert.Len(t, l.Items, 2)
	}
}
