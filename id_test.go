package mongo_test

import (
	"regexp"
	"testing"

	"gopkg.in/mgo.v2/bson"

	mongo "github.com/rs/rest-layer-mongo"
)

const (
	validObjectID   = "59a40602952dbd0001c3ffc9"
	invalidObjectID = "59a40602952dbd0001c3ffc9f"
)

func TestObjectIDValidate(t *testing.T) {
	v := &mongo.ObjectID{}

	t.Run("validObjectID", func(t *testing.T) {
		expect := bson.ObjectIdHex(validObjectID)
		id, err := v.Validate(validObjectID)
		if expect != id {
			t.Errorf("v.Validate(validObjectID):\n %v (expect) != %v (actual)", expect, id)
		}
		if err != nil {
			t.Error("v.Validate(validObjectID):\n unexpected error:", err)
		}
	})

	t.Run("invalidObjectID", func(t *testing.T) {
		id, err := v.Validate(invalidObjectID)
		if nil != id {
			t.Errorf("v.Validate(invalidObjectID):\n %v (expect) != %v (actual)", nil, id)
		}
		if err == nil {
			t.Error("v.Validate(invalidObjectID):\n expected error, got nil")
		}
	})
}

func TestObjectIDJSONSchmea(t *testing.T) {
	v := &mongo.ObjectID{}
	m, err := v.BuildJSONSchema()
	if err != nil {
		t.Error("_, err := v.BuildJSONSchema():\n unexpected error:", err)
	}
	if m == nil {
		t.Fatal("m, _ := v.BuildJSONSchema():\n expected m not to be nil")
	}
	if s := m["type"]; s != "string" {
		t.Fatalf("m, _ := v.BuildJSONSchema(); m[\"type\"]\n %v (expected) != %v (actual)", "string", s)
	}
	re, err := regexp.Compile(m["pattern"].(string))
	if err != nil {
		t.Fatal("_, err := regexp.Compile(m[\"type\"]);\n unexpected error:", m, err)
	}

	t.Run("validObjectID", func(t *testing.T) {
		if match := re.MatchString(validObjectID); !match {
			t.Errorf("re.MatchString(validObjectID)\n %v (expected) != %v (actual)", true, match)
		}
	})

	t.Run("invalidObjectID", func(t *testing.T) {
		if match := re.MatchString(invalidObjectID); match {
			t.Errorf("re.MatchString(invalidObjectID)\n %v (expected) != %v (actual)", false, match)
		}
	})
}
