package model

type Result struct {
	OperationType string
	SQL           []string
	SchemaSQL     string
	CreateSQL     string
	AlterSQL      []string
}
