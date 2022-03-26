package table

import (
	"bytes"
	"errors"
	"reflect"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

// MigrateTables sets target fields to database tables.
func MigrateTables(s interface{}, db kvdb.Store) {
	value := reflect.ValueOf(s).Elem()

	var keys uniqKeys
	defer keys.Check()

	for i := 0; i < value.NumField(); i++ {
		if prefix := value.Type().Field(i).Tag.Get("table"); prefix != "" && prefix != "-" {

			field := value.Field(i)
			var val reflect.Value
			if db != nil {
				keys.Add(prefix)
				table := New(db, []byte(prefix))
				val = reflect.ValueOf(table)
			} else {
				val = reflect.Zero(field.Type())
			}
			field.Set(val)
		}
	}
}

// OpenTables sets target fields to database tables.
func OpenTables(s interface{}, producer kvdb.DBProducer, baseName string) error {
	value := reflect.ValueOf(s).Elem()

	var keys uniqKeys

	for i := 0; i < value.NumField(); i++ {
		if prefix := value.Type().Field(i).Tag.Get("table"); prefix != "" && prefix != "-" {
			keys.Add(prefix)
			db, err := producer.OpenDB(baseName + "/" + prefix)
			if err != nil {
				return err
			}
			value.Field(i).Set(reflect.ValueOf(db))
		}
	}
	return keys.Check()
}

// CloseTables closes target fields databases.
func CloseTables(s interface{}) error {
	value := reflect.ValueOf(s).Elem()

	for i := 0; i < value.NumField(); i++ {
		if prefix := value.Type().Field(i).Tag.Get("table"); prefix != "" {
			field := value.Field(i)

			if field.IsNil() {
				continue
			}

			db := field.Interface().(kvdb.Store)
			err := db.Close()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// MigrateCaches sets target fields to get() result.
func MigrateCaches(c interface{}, get func() interface{}) {
	value := reflect.ValueOf(c).Elem()
	for i := 0; i < value.NumField(); i++ {
		if prefix := value.Type().Field(i).Tag.Get("cache"); prefix != "" {
			field := value.Field(i)
			var cache interface{}
			if get != nil {
				cache = get()
			}
			var val reflect.Value
			if cache != nil {
				val = reflect.ValueOf(cache)
			} else {
				val = reflect.Zero(field.Type())
			}
			field.Set(val)
		}
	}
}

type uniqKeys struct {
	len  int
	keys [][]byte
}

func (u *uniqKeys) Add(s string) {
	key := []byte(s)

	if len(u.keys) == 0 || u.len > len(key) {
		u.len = len(key)
	}
	u.keys = append(u.keys, key)
}

func (u *uniqKeys) Check() error {
	for i := 0; i < len(u.keys); i++ {
		for j := i + 1; j < len(u.keys); j++ {
			a := u.keys[i][:u.len]
			b := u.keys[j][:u.len]
			if bytes.Equal(a, b) {
				return errors.New("prefixes '" + string(u.keys[i]) + "' and '" + string(u.keys[j]) + "' are the same")
			}
		}
	}
	return nil
}
