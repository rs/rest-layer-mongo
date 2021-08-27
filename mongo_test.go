package mongo_test

import (
	"context"
	"math/rand"
	"reflect"
	"testing"
	"time"

	mongo "github.com/rs/rest-layer-mongo"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema/query"
	mgo "gopkg.in/mgo.v2"
)

// Mongo doesn't support nanoseconds
var now = time.Now().Round(time.Millisecond)

func init() {
	rand.Seed(now.UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz")

func randomName(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func setupDBTest(t *testing.T) (*mgo.Session, func()) {
	dbName := randomName(16)
	if testing.Short() {
		t.Skip("skipping DB test in short mode.")
	}
	s, err := mgo.Dial("mongodb:///" + dbName)
	if err != nil {
		t.Fatal("Unexpected error for mgo.Dial:", err)
	}
	return s, cleanup(s, dbName)
}

// cleanup deletes a database immediately and on defer when call as:
//
//   defer cleanup(c, "database")()
func cleanup(s *mgo.Session, db string) func() {
	s.DB(db).DropDatabase()
	return func() {
		s.DB(db).DropDatabase()
	}
}

// asserts that the items in a collection matches the provided list of IDs.
func assertCollectionIDs(t testing.TB, c *mgo.Collection, expect []string) {
	t.Helper()

	var result []string
	if err := c.Find(nil).Distinct("_id", &result); err != nil {
		t.Errorf("Unexpected error for Collection.Find: %v", err)
	}
	if !reflect.DeepEqual(result, expect) {
		t.Errorf("Unexpected IDs inserted;  got: %v want: %v", result, expect)
	}
}

func TestInsert(t *testing.T) {
	s, cleanup := setupDBTest(t)
	defer cleanup()
	h := mongo.NewHandler(s, "", "test")
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
	if err := h.Insert(context.Background(), items); err != nil {
		t.Fatal(err)
	}

	result := map[string]interface{}{}
	if err := s.DB("").C("test").FindId("1234").One(&result); err != nil {
		t.Fatal(err)
	}
	expect := map[string]interface{}{"foo": "bar", "_id": "1234", "_etag": "etag", "_updated": now}
	if !reflect.DeepEqual(expect, result) {
		t.Errorf("got: %v want: %v", result, expect)
	}

	// Inserting same item twice should return a conflict error
	err := h.Insert(context.Background(), items)
	if result, expect := err, resource.ErrConflict; result != expect {
		t.Errorf("got: %v want: %v", result, expect)
	}

}

func TestUpdate(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)

	newItem := &resource.Item{
		ID:      "1234",
		ETag:    "etag2",
		Updated: now,
		Payload: map[string]interface{}{
			"id":  "1234",
			"foo": "baz",
		},
	}
	newItemDoc := map[string]interface{}{
		"_id":      "1234",
		"_etag":    "etag2",
		"_updated": now,
		"foo":      "baz",
	}

	t.Run("when updating a non-existing item", func(t *testing.T) {
		oldItem := &resource.Item{
			ID:      "1234",
			ETag:    "etag1",
			Updated: now,
			Payload: map[string]interface{}{
				"id":  "1234",
				"foo": "bar",
			},
		}

		s, cleanup := setupDBTest(t)
		defer cleanup()
		h := mongo.NewHandler(s, "", "test")

		err := h.Update(context.Background(), newItem, oldItem)

		t.Run("should error", func(t *testing.T) {
			if result, expect := err, resource.ErrNotFound; result != expect {
				t.Errorf("got: %v want: %v", result, expect)
			}
		})
	})

	t.Run("when updating an existing item", func(t *testing.T) {
		oldItem := &resource.Item{
			ID:      "1234",
			ETag:    "etag1",
			Updated: now,
			Payload: map[string]interface{}{
				"id":  "1234",
				"foo": "bar",
			},
		}

		s, cleanup := setupDBTest(t)
		defer cleanup()
		h := mongo.NewHandler(s, "", "test")

		if err := h.Insert(context.Background(), []*resource.Item{oldItem}); err != nil {
			t.Fatal(err)
		}

		if err := h.Update(context.Background(), newItem, oldItem); err != nil {
			t.Fatal(err)
		}

		t.Run("then should have updated the item", func(t *testing.T) {
			result := map[string]interface{}{}

			if err := s.DB("").C("test").FindId("1234").One(&result); err != nil {
				t.Fatal(err)
			}

			if expect := newItemDoc; !reflect.DeepEqual(result, expect) {
				t.Errorf("\ngot: %v\nwant: %v", result, expect)
			}
		})

		t.Run("when attempting the same update again", func(t *testing.T) {
			err := h.Update(context.Background(), newItem, oldItem)

			t.Run("then should return a conflict due to mismatching E-tags", func(t *testing.T) {
				if result, expect := err, resource.ErrConflict; result != expect {
					t.Errorf("got %v want %v", result, expect)
				}
			})
		})
	})

	t.Run("when updating an item that doesn't have an E-Tag set in the DB", func(t *testing.T) {
		oldItem := &resource.Item{
			ID:      "1234",
			ETag:    "p-1234", // Provisional E-tag contains the ID prefixed by "p-"
			Updated: now,
			Payload: map[string]interface{}{
				"id":  "1234",
				"foo": "baz",
			},
		}

		s, cleanup := setupDBTest(t)
		defer cleanup()
		h := mongo.NewHandler(s, "", "test")

		// Inserting directly to the database without setting the _etag field.
		if err := s.DB("").C("test").Insert(map[string]interface{}{"foo": "bar", "_id": "1234", "_updated": now}); err != nil {
			t.Fatal(err)
		}

		if err := h.Update(context.Background(), newItem, oldItem); err != nil {
			t.Fatal(err)
		}

		t.Run("then should have updated the item", func(t *testing.T) {
			result := map[string]interface{}{}

			if err := s.DB("").C("test").FindId("1234").One(&result); err != nil {
				t.Fatal(err)
			}

			if expect := newItemDoc; !reflect.DeepEqual(result, expect) {
				t.Errorf("\ngot: %v\nwant: %v", result, expect)
			}
		})

		t.Run("when attempting the same update again", func(t *testing.T) {
			err := h.Update(context.Background(), newItem, oldItem)

			t.Run("then should return a conflict due to mismatching E-tags", func(t *testing.T) {
				if result, expect := err, resource.ErrConflict; result != expect {
					t.Errorf("got %v want %v", result, expect)
				}
			})
		})
	})
}

func TestDelete(t *testing.T) {
	s, cleanup := setupDBTest(t)
	defer cleanup()
	h := mongo.NewHandler(s, "", "test")
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
	err := h.Delete(context.Background(), item)
	if got, expect := err, resource.ErrNotFound; got != expect {
		t.Errorf("got: %v\nwant: %v\n", got, expect)
	}

	err = h.Insert(context.Background(), []*resource.Item{item})
	if err != nil {
		t.Fatal(err)
	}
	err = h.Delete(context.Background(), item)
	if err != nil {
		t.Fatal(err)
	}

	// Update refused if original item's etag doesn't match stored one
	err = h.Insert(context.Background(), []*resource.Item{item})
	if err != nil {
		t.Fatal(err)
	}
	item.ETag = "etag2"
	err = h.Delete(context.Background(), item)
	if got, expect := err, resource.ErrConflict; got != expect {
		t.Errorf("got %v want: %v", got, expect)
	}

	c := s.DB("").C("testEtag")
	// Add an item without _etag field
	c.Insert(map[string]interface{}{"foo": "bar", "_id": "1234", "_updated": now})
	c.Insert(map[string]interface{}{"foo": "bar", "_id": "12345", "_etag": "etag", "_updated": now})
	h2 := mongo.NewHandler(s, "", "testEtag")
	// A item without _etag field, is extracted with ETag in "p-[id]" format
	originalItem := &resource.Item{
		ID:      "1234",
		ETag:    "p-1234",
		Updated: now,
		Payload: map[string]interface{}{
			"id":  "1234",
			"foo": "baz",
		},
	}
	// Delete an original item with Etag over item in DB without _etag
	err = h2.Delete(context.Background(), originalItem)
	if err != nil {
		t.Fatal(err)
	}

	originalItem.ID = "12345"
	// Delete an original item with Etag over item in DB with _etag
	// fails because _etag is present
	err = h2.Delete(context.Background(), originalItem)
	if got, expect := err, resource.ErrConflict; got != expect {
		t.Errorf("got: %v want: %v", got, expect)
	}
}

func TestClear(t *testing.T) {
	const (
		cName = "test"
	)

	s, cleanup := setupDBTest(t)
	defer cleanup()
	h := mongo.NewHandler(s, "", cName)
	items := []*resource.Item{
		{ID: "1", Payload: map[string]interface{}{"id": "1", "name": "a"}},
		{ID: "2", Payload: map[string]interface{}{"id": "2", "name": "b"}},
		{ID: "3", Payload: map[string]interface{}{"id": "3", "name": "c"}},
		{ID: "4", Payload: map[string]interface{}{"id": "4", "name": "d"}},
	}
	if err := h.Insert(context.Background(), items); err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	q, err := query.New("", `{name:{$in:["c","d"]}}`, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := h.Clear(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if expect := 2; deleted != expect {
		t.Errorf("Unexpected result:\nexpect: %#v\ngot: %#v", expect, deleted)
	}
	assertCollectionIDs(t, s.DB("").C(cName), []string{"1", "2"})

	q, err = query.New("", `{id:"2"}`, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err = h.Clear(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if expect := 1; deleted != expect {
		t.Errorf("Unexpected result:\nexpect: %#v\ngot: %#v", expect, deleted)
	}
	assertCollectionIDs(t, s.DB("").C(cName), []string{"1"})
}
func TestClearLimit(t *testing.T) {
	const (
		dbName = "testclearlimit"
		cName  = "test"
	)

	s, cleanup := setupDBTest(t)
	defer cleanup()

	h := mongo.NewHandler(s, "", cName)
	items := []*resource.Item{
		{ID: "1", Payload: map[string]interface{}{"id": "1", "name": "a"}},
		{ID: "2", Payload: map[string]interface{}{"id": "2", "name": "b"}},
		{ID: "3", Payload: map[string]interface{}{"id": "3", "name": "d"}}, // should be sorted after 4
		{ID: "4", Payload: map[string]interface{}{"id": "4", "name": "c"}}, // should be removed
	}
	if err := h.Insert(context.Background(), items); err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	q, err := query.New("", `{name:{$in:["c","d"]}}`, "name", &query.Window{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := h.Clear(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if expect := 1; deleted != expect {
		t.Errorf("Unexpected result:\nexpect: %#v\ngot: %#v", expect, deleted)
	}
	assertCollectionIDs(t, s.DB("").C(cName), []string{"1", "2", "3"})
}

func TestClearOffset(t *testing.T) {
	const (
		cName = "test"
	)

	s, cleanup := setupDBTest(t)
	defer cleanup()
	h := mongo.NewHandler(s, "", cName)
	items := []*resource.Item{
		{ID: "1", Payload: map[string]interface{}{"id": "1", "name": "a"}},
		{ID: "2", Payload: map[string]interface{}{"id": "2", "name": "b"}},
		{ID: "3", Payload: map[string]interface{}{"id": "3", "name": "d"}}, // should be sorted after 4, should be removed
		{ID: "4", Payload: map[string]interface{}{"id": "4", "name": "c"}}, // should be skipped
	}
	if err := h.Insert(context.Background(), items); err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	q, err := query.New("", `{name:{$in:["c","d"]}}`, "name", &query.Window{Offset: 1})
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := h.Clear(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if expect := 1; deleted != expect {
		t.Errorf("Unexpected result:\nexpect: %#v\ngot: %#v", expect, deleted)
	}
	assertCollectionIDs(t, s.DB("").C(cName), []string{"1", "2", "4"})
}

func TestFind(t *testing.T) {
	allItems := []*resource.Item{
		{ID: "1", Payload: map[string]interface{}{"id": "1", "name": "a", "age": 1}},
		{ID: "2", Payload: map[string]interface{}{"id": "2", "name": "b", "age": 1}},
		{ID: "3", Payload: map[string]interface{}{"id": "3", "name": "c", "age": 2}},
		{ID: "4", Payload: map[string]interface{}{"id": "4", "name": "d", "age": 2}},
		{ID: "5", Payload: map[string]interface{}{"id": "5", "name": "rest-layer-regexp"}},
		{ID: "6", Payload: map[string]interface{}{"id": "6", "name": "f",
			"arr": []interface{}{
				map[string]interface{}{"a": "foo", "b": "bar"},
				map[string]interface{}{"a": "foo", "b": "baz"},
			},
		}},
	}
	doPositiveFindTest := func(t *testing.T, h mongo.Handler, q *query.Query) *resource.ItemList {
		l, err := h.Find(context.Background(), q)

		if err != nil {
			t.Fatal(err)
		}

		if l == nil {
			t.Fatal("Unexpected nil result for Handler.Find")
		}
		return l
	}
	totalCheckFunc := func(expect int, list *resource.ItemList) func(t *testing.T) {
		return func(t *testing.T) {
			t.Helper()

			if result := list.Total; result != expect {
				t.Errorf("got: %d want: %d", result, expect)
			}
		}
	}
	itemsCheckLenFunc := func(expect int, list *resource.ItemList) func(t *testing.T) {
		return func(t *testing.T) {
			t.Helper()
			if result := len(list.Items); result != expect {
				t.Errorf("got: %d want: %d", result, expect)
			}
		}
	}
	itemsCheckFunc := func(expect []*resource.Item, list *resource.ItemList) func(t *testing.T) {
		return func(t *testing.T) {
			t.Helper()

			if result := list.Items; !reflect.DeepEqual(result, expect) {
				t.Errorf("\ngot: %v\nwant: %v\n", result, expect)
			}
		}
	}

	s, cleanup := setupDBTest(t)
	defer cleanup()
	h := mongo.NewHandler(s, "", "test")

	if err := h.Insert(context.Background(), allItems); err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	t.Run("when using an empty query", func(t *testing.T) {
		l := doPositiveFindTest(t, h, &query.Query{})

		t.Run("then ItemList.Total should count all items", totalCheckFunc(len(allItems), l))
		t.Run("then ItemList.Items should include all items", itemsCheckLenFunc(len(allItems), l))
		// Do not check items content as ordering could vary.
	})
	t.Run("when setting limit to 0", func(t *testing.T) {
		l := doPositiveFindTest(t, h, &query.Query{
			Window: &query.Window{Limit: 0}},
		)

		t.Run("then ItemList.Total should count all items", totalCheckFunc(len(allItems), l))
		expectItems := []*resource.Item{}
		t.Run("then ItemList.Items should be an empty list", itemsCheckFunc(expectItems, l))
	})
	t.Run("when setting limit -1 and offset to 2", func(t *testing.T) {
		l := doPositiveFindTest(t, h, &query.Query{
			Window: &query.Window{Limit: -1, Offset: 2},
		})

		t.Run("then ItemList.Total should count all items", totalCheckFunc(len(allItems), l))

		t.Run("then ItemList.Items should include all items except the first two", itemsCheckLenFunc(len(allItems)-2, l))
		// Do not check result's content as its order is unpredictable.
	})
	t.Run("when setting limit -1 and offset matching the length of all items", func(t *testing.T) {
		l := doPositiveFindTest(t, h, &query.Query{
			Window: &query.Window{Limit: -1, Offset: len(allItems)},
		})

		// Not able to get total count in one query.
		t.Run("then ItemList.Total should not be deduced", totalCheckFunc(-1, l))
		// Check that we get an empty Item list and not nil.
		expectItems := []*resource.Item{}
		t.Run("then ItemList.Items should be an empty list", itemsCheckFunc(expectItems, l))
	})

	t.Run("when setting limit -1 and offset matching the length of all items +1", func(t *testing.T) {
		l := doPositiveFindTest(t, h, &query.Query{
			Window: &query.Window{Limit: -1, Offset: len(allItems)},
		})

		// Not able to get total count in one query.
		t.Run("then ItemList.Total should not be deduced", totalCheckFunc(-1, l))
		// Check that we get an empty Item list and not nil.
		expectItems := []*resource.Item{}
		t.Run("then ItemList.Items should be an empty list", itemsCheckFunc(expectItems, l))
	})
	t.Run("when querying for a specific field value with limit 1", func(t *testing.T) {
		l := doPositiveFindTest(t, h, &query.Query{
			Predicate: query.MustParsePredicate(`{name:"c"}`),
			Window:    &query.Window{Limit: 1, Offset: 0},
		})

		// Not able to get total count in one query.
		t.Run("then ItemList.Total should not be deduced", totalCheckFunc(-1, l))

		expectItems := []*resource.Item{
			{ID: "3", ETag: "p-3", Payload: map[string]interface{}{"id": "3", "name": "c", "age": 2}},
		}
		t.Run("then ItemList.Items should contain the matching item", itemsCheckFunc(expectItems, l))
	})
	t.Run("when querying for a field using the $in operator and limit 100 and a projection", func(t *testing.T) {
		l := doPositiveFindTest(t, h, &query.Query{
			Predicate:  query.MustParsePredicate(`{name:{$in:["c","d"]}}`),
			Window:     &query.Window{Limit: 100, Offset: 0},
			Projection: query.MustParseProjection("name"),
		})
		t.Run("then ItemList.Total should be deduced correctly", totalCheckFunc(2, l))

		expectItems := []*resource.Item{
			{ID: "3", ETag: "p-3", Payload: map[string]interface{}{"id": "3", "name": "c", "age": 2}},
			{ID: "4", ETag: "p-4", Payload: map[string]interface{}{"id": "4", "name": "d", "age": 2}},
		}
		t.Run("then ItemList.Items should include all matching items and ignore projection", itemsCheckFunc(expectItems, l))
	})
	t.Run("when querying for an existing ID", func(t *testing.T) {
		l := doPositiveFindTest(t, h, &query.Query{
			Predicate: query.MustParsePredicate(`{id:"3"}`)},
		)

		t.Run("then ItemList.Total should be deduced correctly", totalCheckFunc(1, l))

		expectItems := []*resource.Item{
			{ID: "3", ETag: "p-3", Payload: map[string]interface{}{"id": "3", "name": "c", "age": 2}},
		}
		t.Run("then ItemList.Items should include the matching item", itemsCheckFunc(expectItems, l))
	})
	t.Run("when querying using regex", func(t *testing.T) {
		l := doPositiveFindTest(t, h, &query.Query{
			Predicate: query.MustParsePredicate(`{name:{$regex:"^re[s]{1}t-.+yer.+exp$"}}`),
		})
		t.Run("then ItemList.Total should be deduced correctly", totalCheckFunc(1, l))

		expectItems := []*resource.Item{
			{ID: "5", ETag: "p-5", Payload: map[string]interface{}{"id": "5", "name": "rest-layer-regexp"}},
		}
		t.Run("then ItemList.Items should include the matching item", itemsCheckFunc(expectItems, l))
	})
	t.Run("when querying for a non-existant ID with limit 1", func(t *testing.T) {
		l := doPositiveFindTest(t, h, &query.Query{
			Predicate: query.MustParsePredicate(`{id:"10"}`),
			Window:    &query.Window{Limit: 1, Offset: 0},
		})
		t.Run("then ItemList.Total should be deduced to 0", totalCheckFunc(0, l))
		t.Run("then ItemList.Items should be an empty list", itemsCheckFunc([]*resource.Item{}, l))
	})
	t.Run("when querying for both existing and non-existant IDs with limit 1", func(t *testing.T) {
		l := doPositiveFindTest(t, h, &query.Query{
			Predicate: query.MustParsePredicate(`{id:{$in:["3","4","10"]}}`),
			Window:    &query.Window{Limit: 1, Offset: 0},
		})

		// Not able to get total count in one query.
		t.Run("then ItemList.Total should not be deduced", totalCheckFunc(-1, l))

		expectItems := []*resource.Item{
			{ID: "3", ETag: "p-3", Payload: map[string]interface{}{"id": "3", "name": "c", "age": 2}},
		}
		t.Run("then ItemList.Items should include the first matching item", itemsCheckFunc(expectItems, l))
	})
	t.Run("when querying for both existing and non-existant IDs", func(t *testing.T) {
		l := doPositiveFindTest(t, h, &query.Query{
			Predicate: query.MustParsePredicate(`{id:{$in:["3","4","10"]}}`),
		})

		t.Run("then ItemList.Total should be deduced correctly", totalCheckFunc(2, l))

		expectItems := []*resource.Item{
			{ID: "3", ETag: "p-3", Payload: map[string]interface{}{"id": "3", "name": "c", "age": 2}},
			{ID: "4", ETag: "p-4", Payload: map[string]interface{}{"id": "4", "name": "d", "age": 2}},
		}
		t.Run("then ItemList.Items should include all matching items", itemsCheckFunc(expectItems, l))
	})
	t.Run("when quering for array of objects match", func(t *testing.T) {
		l := doPositiveFindTest(t, h, &query.Query{
			Predicate: query.MustParsePredicate(`{arr:{$elemMatch:{a:"foo"}}}`),
		})

		t.Run("then ItemList.Total should be deduced correctly", totalCheckFunc(1, l))

		expectItems := []*resource.Item{
			{ID: "6", ETag: "p-6", Payload: map[string]interface{}{"id": "6", "name": "f",
				"arr": []interface{}{
					map[string]interface{}{"a": "foo", "b": "bar"},
					map[string]interface{}{"a": "foo", "b": "baz"},
				},
			}},
		}
		t.Run("then ItemList.Items should include the first matching item", itemsCheckFunc(expectItems, l))
	})
	t.Run("when quering with equivalent $and queries", func(t *testing.T) {
		equivalents := []struct {
			name      string
			predicate query.Predicate
		}{
			{
				name: "implicit and predicate",
				predicate: query.Predicate{
					&query.Equal{Field: "age", Value: 1},
					&query.Equal{Field: "name", Value: "b"},
				},
			},
			{
				name: "explicit $and",
				predicate: query.Predicate{
					&query.And{
						&query.Equal{Field: "age", Value: 1},
						&query.Equal{Field: "name", Value: "b"},
					},
				},
			},
			{
				name: "explicit &or of implicit and predicate",
				predicate: query.Predicate{
					&query.Or{
						query.Predicate{
							&query.Equal{Field: "age", Value: 1},
							&query.Equal{Field: "name", Value: "b"},
						},
					},
				},
			},
			{
				name: "explicit $and of predicates",
				predicate: query.Predicate{
					&query.And{
						query.Predicate{&query.Equal{Field: "age", Value: 1}},
						query.Predicate{&query.Equal{Field: "name", Value: "b"}},
					},
				},
			},
		}
		for _, tc := range equivalents {
			t.Run(tc.name, func(t *testing.T) {
				l := doPositiveFindTest(t, h, &query.Query{
					Predicate: tc.predicate,
				})
				t.Run("then ItemList.Total should be deduced correctly", totalCheckFunc(1, l))

				expectItems := []*resource.Item{
					{ID: "2", ETag: "p-2", Payload: map[string]interface{}{"id": "2", "name": "b", "age": 1}},
				}
				t.Run("then ItemList.Items should include the matching item", itemsCheckFunc(expectItems, l))
			})
		}
	})
}
