package model

import (
	"fmt"
	"sync"
)

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
