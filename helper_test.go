package db

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type ManyTypes struct {
	id      int `partition_key:"1"`
	name    string
	email   string
	weight  float64
	address []string
	parents [2]string
	tmp     map[string]int
}

type KS struct {
	id int `partition_key:"1"`
}

func TestDef(t *testing.T) {
	ass := assert.New(t)
	u := NewModel(&ManyTypes{}, "")

	ass.Equal("ManyTypes", u.name)
	ass.Equal("", u.keyspace)
}

func TestCreateTable_type(t *testing.T) {
	ass := assert.New(t)
	u := NewModel(&ManyTypes{}, "")

	stmt, err := u.CreateTable()
	ass.Equal(nil, err)
	ass.Equal("CREATE TABLE IF NOT EXISTS ManyTypes("+
		"id int,"+
		"name text,"+
		"email text,"+
		"weight double,"+
		"address list<text>,"+
		"parents list<text>,"+
		"tmp map<text,int>,"+
		"PRIMARY KEY (id))", stmt)
}

func TestCreateTable_keyspace(t *testing.T) {
	ass := assert.New(t)
	k := NewModel(&KS{}, "test")

	stmt, err := k.CreateTable()
	ass.Equal(nil, err)
	ass.Equal("CREATE TABLE IF NOT EXISTS test.KS(id int,PRIMARY KEY (id))", stmt)
}

type CounterTable struct {
	id    int `partition_key:"1"`
	likes COUNTER
}

func TestCreateTable_counter(t *testing.T) {
	ass := assert.New(t)
	m := NewModel(&CounterTable{}, "")

	stmt, err := m.CreateTable()
	ass.Equal(nil, err)
	ass.Equal("CREATE TABLE IF NOT EXISTS CounterTable("+
		"id int,"+
		"likes counter,"+
		"PRIMARY KEY (id))", stmt)
}

type OnePkOneCk struct {
	id  int `partition_key:"1"`
	id2 int `partition_key:"2"`
}

func TestCreateTable_1Pk_1Ck(t *testing.T) {
	ass := assert.New(t)
	m := NewModel(&OnePkOneCk{}, "")

	stmt, err := m.CreateTable()
	ass.Equal(nil, err)
	ass.Equal("CREATE TABLE IF NOT EXISTS OnePkOneCk("+
		"id int,"+
		"id2 int,"+
		"PRIMARY KEY (id,id2))", stmt)
}

type OnePkTwoCk struct {
	id  int `partition_key:"1"`
	id2 int `cluster_key:"1"`
	id3 int `cluster_key:"2"`
}

func TestCreateTable_1Pk_2Ck(t *testing.T) {
	ass := assert.New(t)
	m := NewModel(&OnePkTwoCk{}, "")

	stmt, err := m.CreateTable()
	ass.Equal(nil, err)
	ass.Equal("CREATE TABLE IF NOT EXISTS OnePkTwoCk("+
		"id int,"+
		"id2 int,"+
		"id3 int,"+
		"PRIMARY KEY (id,id2,id3))", stmt)
}

type TwoPkTwoCk struct {
	id  int `partition_key:"1"`
	id2 int `partition_key:"2"`
	id3 int `cluster_key:"1"`
	id4 int `cluster_key:"2"`
}

func TestCreateTable_2Pk_2Ck(t *testing.T) {
	ass := assert.New(t)
	m := NewModel(&TwoPkTwoCk{}, "")

	stmt, err := m.CreateTable()
	ass.Equal(nil, err)
	ass.Equal("CREATE TABLE IF NOT EXISTS TwoPkTwoCk("+
		"id int,"+
		"id2 int,"+
		"id3 int,"+
		"id4 int,"+
		"PRIMARY KEY ((id,id2),id3,id4))", stmt)
}
