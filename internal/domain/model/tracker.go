package model

import (
	"fmt"
	"sync"
)

type QueryType string

const (
	INSERT        QueryType = "INSERT"
	UPDATE        QueryType = "UPDATE"
	DELETE        QueryType = "DELETE"
	CREATE_TABLE  QueryType = "CREATE_TABLE"
	ALTER_TABLE   QueryType = "ALTER_TABLE"
	CREATE_SCHEMA QueryType = "CREATE_SCHEMA"
)

type QueryTracker struct {
	Type    QueryType
	Query   string
	Columns []string
}

type Tracker struct {
	M sync.Map
}

func NewTracker() *Tracker {
	return &Tracker{
		M: sync.Map{},
	}
}

func (t *Tracker) Get(key string) (interface{}, bool) {
	val, ok := t.M.Load(key)

	if !ok {
		fmt.Printf("Not found %v", key)
		return "", false
	}
	return val, true
}

func (t *Tracker) Store(key string, val interface{}) {
	t.M.Store(key, val)
}
