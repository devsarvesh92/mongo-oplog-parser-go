package parser

import (
	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
	"github.com/devsarvesh92/mongoOplogParser/internal/domain/service/strategy"
)

const (
	Float   = "FLOAT"
	VARCHAR = "VARCHAR(255)"
	BOOL    = "BOOLEAN"
)

// GenerateSQL transforms a set of MongoDB oplogs into SQL statements.
// It analyzes each oplog and generates the appropriate SQL commands including
// schema creation, table creation, inserts, updates, and deletes.
func GenerateSQL(oplogs []model.Oplog) (result model.Result) {
	if len(oplogs) == 0 {
		return
	}

	queryTracker := make(map[string]model.QueryTracker)
	insertStrategy := strategy.NewInsertStrategy()
	updateStrategy := strategy.NewUpdateStrategy()
	deleteStrategy := strategy.NewDeleteStrategy()
	nestedInsertStrategy := strategy.NewNestedInsertStragey()

	for _, oplog := range oplogs {
		switch {
		case oplog.IsNestedDocument():
			result.SQL = append(result.SQL, nestedInsertStrategy.Generate(oplog, queryTracker)...)
		case oplog.IsInsert():
			result.SQL = append(result.SQL, insertStrategy.Generate(oplog, queryTracker)...)
		case oplog.IsUpdate():
			updateSQL := updateStrategy.Generate(oplog, queryTracker)
			if updateSQL != "" {
				result.SQL = append(result.SQL, updateSQL)
			}
		case oplog.IsDelete():
			deleteSQL := deleteStrategy.Generate(oplog, queryTracker)
			if deleteSQL != "" {
				result.SQL = append(result.SQL, deleteSQL)
			}
		}
		result.OperationType = string(oplog.GetOperationType())
	}
	return
}
