/*
	helper functions
*/

package db

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
)

type COUNTER int64

type Model struct {
	name     string // table name
	keyspace string // keyspace name

	d reflect.Type // type info of struct instance to represent table definition
}

type keyDecl struct {
	name string
	idx  int
}

type keyDecls []*keyDecl

//
//	interface for Sorting keyDecl
//
func (k keyDecls) Len() int {
	return len(k)
}

func (k keyDecls) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}

func (k keyDecls) Less(i, j int) bool {
	return k[i].idx < k[j].idx
}

func convertToColumnType(t reflect.Type) (string, error) {
	// for user-definded type
	n := t.Name()
	switch n {
	case "UUID":
		return "uuid", nil
	case "COUNTER":
		return "counter", nil
	default:
		switch t.Kind() {
		case reflect.String:
			return "text", nil
		case reflect.Int, reflect.Int32:
			return "int", nil
		case reflect.Float32:
			return "float", nil
		case reflect.Float64:
			return "double", nil
		case reflect.Array, reflect.Slice:
			if name, err := convertToColumnType(t.Elem()); err != nil {
				return "", err
			} else {
				return "list<" + name + ">", nil
			}
		case reflect.Map:
			if k, err := convertToColumnType(t.Key()); err != nil {
				return "", err
			} else {
				if v, err := convertToColumnType(t.Elem()); err != nil {
					return "", err
				} else {
					return "map<" + k + "," + v + ">", nil
				}
			}

		default:
			return "", errors.New(fmt.Sprintf("Unsupported Type: %v, %v", n, t.Kind()))
		}
	}
}

//
//	Exported Function
//

func (d *Model) CreateTable() (stmt string, err error) {
	// reset return
	stmt = "CREATE TABLE IF NOT EXISTS"

	// table name
	stmt += " "
	if len(d.keyspace) > 0 {
		stmt += d.keyspace + "."
	}
	stmt += d.name

	var col, type_name string
	pk := make(keyDecls, 0, 10)
	ck := make(keyDecls, 0, 10)

	for i := 0; i < d.d.NumField(); i++ {
		if err != nil {
			break
		}
		f := d.d.Field(i)

		// column definition
		type_name, err = convertToColumnType(f.Type)
		if err != nil {
			break
		}

		col += f.Name + " " + type_name + ","

		// partition key
		{
			if k := f.Tag.Get("partition_key"); len(k) > 0 {
				if idx, err := strconv.Atoi(k); err == nil {
					pk = append(pk, &keyDecl{f.Name, idx})
				}
			}

			if err != nil {
				break
			}
		}

		// clustering key
		{
			if k := f.Tag.Get("cluster_key"); len(k) > 0 {
				if idx, err := strconv.Atoi(k); err == nil {
					ck = append(ck, &keyDecl{f.Name, idx})
				}
			}

			if err != nil {
				break
			}
		}
	}

	if err != nil {
		return "", err
	}

	stmt += "(" + col

	// keys
	genKeyStmt := func(k keyDecls) (s string) {
		sort.Sort(k)
		for _, v := range k {
			if len(s) > 0 {
				s += ","
			}
			s += v.name
		}
		return s
	}

	pk_stmt := genKeyStmt(pk)
	ck_stmt := genKeyStmt(ck)
	if len(pk_stmt) > 0 {
		stmt += "PRIMARY KEY ("
		if len(ck_stmt) > 0 {
			if len(pk) == 1 {
				stmt += pk_stmt + "," + ck_stmt + ")"
			} else {
				stmt += "(" + pk_stmt + ")," + ck_stmt + ")"
			}
		} else {
			stmt += pk_stmt + ")"
		}
	} else {
		return "", errors.New(fmt.Sprintf("at least 1 primary key required."))
	}

	stmt += ")"

	return stmt, err
}

func (d *Model) Keyspace(name string) {
	d.keyspace = name
}

func NewModel(table_def interface{}, keyspace string) (d *Model) {
	t := reflect.TypeOf(table_def)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return NewModelWithName(table_def, t.Name(), keyspace)
}

func NewModelWithName(table_def interface{}, name string, keyspace string) (d *Model) {
	t := reflect.TypeOf(table_def)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		// TODO: panic
	}
	return &Model{
		name:     name,
		keyspace: keyspace,
		d:        t,
	}
}
