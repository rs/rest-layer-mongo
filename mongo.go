// Package mongo is a REST Layer resource storage handler for MongoDB using mgo
package mongo

import (
	"context"
	"time"

	"github.com/rs/rest-layer/resource"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// mongoItem is a bson representation of a resource.Item
type mongoItem struct {
	ID      interface{}            `bson:"_id"`
	ETag    string                 `bson:"_etag"`
	Updated time.Time              `bson:"_updated"`
	Payload map[string]interface{} `bson:",inline"`
}

// newMongoItem converts a resource.Item into a mongoItem
func newMongoItem(i *resource.Item) *mongoItem {
	// Filter out id from the payload so we don't store it twice
	p := map[string]interface{}{}
	for k, v := range i.Payload {
		if k != "id" {
			p[k] = v
		}
	}
	return &mongoItem{
		ID:      i.ID,
		ETag:    i.ETag,
		Updated: i.Updated,
		Payload: p,
	}
}

// newItem converts a back mongoItem into a resource.Item
func newItem(i *mongoItem) *resource.Item {
	// Add the id back (we use the same map hoping the mongoItem won't be stored back)
	i.Payload["id"] = i.ID
	return &resource.Item{
		ID:      i.ID,
		ETag:    i.ETag,
		Updated: i.Updated,
		Payload: i.Payload,
	}
}

// Handler handles resource storage in a MongoDB collection.
type Handler func(ctx context.Context) (*mgo.Collection, error)

// NewHandler creates an new mongo handler
func NewHandler(s *mgo.Session, db, collection string) Handler {
	return func(ctx context.Context) (*mgo.Collection, error) {
		// With mgo, session.Copy() pulls a connection from the connection pool
		s := s.Copy()
		return s.DB(db).C(collection), nil
	}
}

// C returns the mongo collection managed by this storage handler
// from a Copy() of the mgo session.
func (m Handler) c(ctx context.Context) (*mgo.Collection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c, err := m(ctx)
	if err != nil {
		return nil, err
	}
	// Ensure safe mode is enabled in order to get errors
	c.Database.Session.EnsureSafe(&mgo.Safe{})
	// Set a timeout to match the context deadline if any
	if deadline, ok := ctx.Deadline(); ok {
		timeout := deadline.Sub(time.Now())
		if timeout <= 0 {
			timeout = 0
		}
		c.Database.Session.SetSocketTimeout(timeout)
		c.Database.Session.SetSyncTimeout(timeout)
	}
	return c, nil
}

// close returns a mgo.Collection's session to the connection pool.
func (m Handler) close(c *mgo.Collection) {
	c.Database.Session.Close()
}

// Insert inserts new items in the mongo collection
func (m Handler) Insert(ctx context.Context, items []*resource.Item) error {
	mItems := make([]interface{}, len(items))
	for i, item := range items {
		mItems[i] = newMongoItem(item)
	}
	c, err := m.c(ctx)
	if err != nil {
		return err
	}
	defer m.close(c)
	err = c.Insert(mItems...)
	if mgo.IsDup(err) {
		// Duplicate ID key
		err = resource.ErrConflict
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// Update replace an item by a new one in the mongo collection
func (m Handler) Update(ctx context.Context, item *resource.Item, original *resource.Item) error {
	mItem := newMongoItem(item)
	c, err := m.c(ctx)
	if err != nil {
		return err
	}
	defer m.close(c)
	err = c.Update(bson.M{"_id": original.ID, "_etag": original.ETag}, mItem)
	if err == mgo.ErrNotFound {
		// Determine if the item is not found or if the item is found but etag missmatch
		var count int
		count, err = c.FindId(original.ID).Count()
		if err != nil {
			// The find returned an unexpected err, just forward it with no mapping
		} else if count == 0 {
			err = resource.ErrNotFound
		} else if ctx.Err() != nil {
			err = ctx.Err()
		} else {
			// If the item were found, it means that its etag didn't match
			err = resource.ErrConflict
		}
	}
	return err
}

// Delete deletes an item from the mongo collection
func (m Handler) Delete(ctx context.Context, item *resource.Item) error {
	c, err := m.c(ctx)
	if err != nil {
		return err
	}
	defer m.close(c)
	err = c.Remove(bson.M{"_id": item.ID, "_etag": item.ETag})
	if err == mgo.ErrNotFound {
		// Determine if the item is not found or if the item is found but etag missmatch
		var count int
		count, err = c.FindId(item.ID).Count()
		if err != nil {
			// The find returned an unexpected err, just forward it with no mapping
		} else if count == 0 {
			err = resource.ErrNotFound
		} else if ctx.Err() != nil {
			err = ctx.Err()
		} else {
			// If the item were found, it means that its etag didn't match
			err = resource.ErrConflict
		}
	}
	return err
}

// Clear clears all items from the mongo collection matching the lookup
func (m Handler) Clear(ctx context.Context, lookup *resource.Lookup) (int, error) {
	q, err := getQuery(lookup)
	if err != nil {
		return 0, err
	}
	c, err := m.c(ctx)
	if err != nil {
		return 0, err
	}
	defer m.close(c)
	info, err := c.RemoveAll(q)
	if err != nil {
		return 0, err
	}
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	return info.Removed, nil
}

// Find items from the mongo collection matching the provided lookup
func (m Handler) Find(ctx context.Context, lookup *resource.Lookup, offset, limit int) (*resource.ItemList, error) {
	q, err := getQuery(lookup)
	if err != nil {
		return nil, err
	}
	s := getSort(lookup)
	c, err := m.c(ctx)
	if err != nil {
		return nil, err
	}
	defer m.close(c)
	var mItem mongoItem
	query := c.Find(q).Sort(s...)

	if offset > 0 {
		query.Skip(offset)
	}
	if limit >= 0 {
		query.Limit(limit)
	}
	// Apply context deadline if any
	if dl, ok := ctx.Deadline(); ok {
		dur := dl.Sub(time.Now())
		if dur < 0 {
			dur = 0
		}
		query.SetMaxTime(dur)
	}
	// Perform request
	iter := query.Iter()
	// Total is set to -1 because we have no easy way with Mongodb to to compute this value
	// without performing two requests.
	list := &resource.ItemList{Total: -1, Items: []*resource.Item{}}
	for iter.Next(&mItem) {
		// Check if context is still ok before to continue
		if err = ctx.Err(); err != nil {
			// TODO bench this as net/context is using mutex under the hood
			iter.Close()
			return nil, err
		}
		list.Items = append(list.Items, newItem(&mItem))
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	// If the number of returned elements is lower than requested limit, or not
	// limit is requested, we can deduce the total number of element for free.
	if limit == -1 || len(list.Items) < limit {
		list.Total = offset + len(list.Items)
	}
	return list, err
}

// Count counts the number items matching the lookup filter
func (m Handler) Count(ctx context.Context, lookup *resource.Lookup) (int, error) {
	q, err := getQuery(lookup)
	if err != nil {
		return -1, err
	}
	c, err := m.c(ctx)
	if err != nil {
		return -1, err
	}
	defer m.close(c)
	query := c.Find(q)
	// Apply context deadline if any
	if dl, ok := ctx.Deadline(); ok {
		dur := dl.Sub(time.Now())
		if dur < 0 {
			dur = 0
		}
		query.SetMaxTime(dur)
	}
	return query.Count()
}
