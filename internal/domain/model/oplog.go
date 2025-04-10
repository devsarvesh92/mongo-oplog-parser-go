package model

import (
	"errors"
	"strings"
)

type OperationType string

const (
	OpInsert OperationType = "i"
	OpUpdate OperationType = "u"
	OpDelete OperationType = "d"
)

type Oplog struct {
	Op string                 `"json:op"`
	Ns string                 `"json:ns"`
	O  map[string]interface{} `"json:o"`
	O2 map[string]interface{} `"json:o2"`
}

func (o *Oplog) GetDatabaseName() (string, error) {
	splitedStrs := strings.Split(o.Ns, ".")

	if len(splitedStrs) <= 1 {
		return "", errors.New("invalid value of namespace")
	}
	return splitedStrs[0], nil
}

func (o *Oplog) GetTableName() (string, error) {
	splitedStrs := strings.Split(o.Ns, ".")

	if len(splitedStrs) <= 1 {
		return "", errors.New("invalid value of table")
	}
	return o.Ns, nil
}

func (o *Oplog) GetOperationType() OperationType {
	return OperationType(o.Op)
}

func (o *Oplog) IsInsert() bool {
	return o.Op == string(OpInsert)
}

func (o *Oplog) IsUpdate() bool {
	return o.Op == string(OpUpdate)
}

func (o *Oplog) IsDelete() bool {
	return o.Op == string(OpDelete)
}
