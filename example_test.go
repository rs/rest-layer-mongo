package mongo_test

import (
	"log"
	"net/http"

	"github.com/rs/cors"
	"github.com/rs/rest-layer-mongo"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/rest"
	"github.com/rs/rest-layer/schema"
	"gopkg.in/mgo.v2"
)

var (
	user = schema.Schema{
		"id":      schema.IDField,
		"created": schema.CreatedField,
		"updated": schema.UpdatedField,
		"name": schema.Field{
			Required:   true,
			Filterable: true,
			Sortable:   true,
			Validator: &schema.String{
				MaxLen: 150,
			},
		},
	}

	// Define a post resource schema
	post = schema.Schema{
		"id":      schema.IDField,
		"created": schema.CreatedField,
		"updated": schema.UpdatedField,
		"user": schema.Field{
			Required:   true,
			Filterable: true,
			Validator: &schema.Reference{
				Path: "users",
			},
		},
		"public": schema.Field{
			Filterable: true,
			Validator:  &schema.Bool{},
		},
		"meta": schema.Field{
			Schema: &schema.Schema{
				"title": schema.Field{
					Required: true,
					Validator: &schema.String{
						MaxLen: 150,
					},
				},
				"body": schema.Field{
					Validator: &schema.String{
						MaxLen: 100000,
					},
				},
			},
		},
	}
)

func Example() {
	session, err := mgo.Dial("")
	if err != nil {
		log.Fatalf("Can't connect to MongoDB: %s", err)
	}
	db := "test_rest_layer"

	index := resource.NewIndex()

	users := index.Bind("users", resource.New(user, mongo.NewHandler(session, db, "users"), resource.Conf{
		AllowedModes: resource.ReadWrite,
	}))

	users.Bind("posts", "user", resource.New(post, mongo.NewHandler(session, db, "posts"), resource.Conf{
		AllowedModes: resource.ReadWrite,
	}))

	api, err := rest.NewHandler(index)
	if err != nil {
		log.Fatalf("Invalid API configuration: %s", err)
	}

	http.Handle("/", cors.New(cors.Options{OptionsPassthrough: true}).Handler(api))

	log.Print("Serving API on http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
