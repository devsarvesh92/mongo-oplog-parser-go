package model

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
