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
	schemaStrategy := strategy.NewSchemaStrategy()
	tableStrategy := strategy.NewTableStrategy()
	alterStrategy := strategy.NewAlterStrategy()

	for _, oplog := range oplogs {
		switch {
		case oplog.IsInsert():
			result.SQL = append(result.SQL, insertStrategy.Generate(oplog, queryTracker))

			schemaSQL := schemaStrategy.Generate(oplog, queryTracker)
			if schemaSQL != "" {
				result.SchemaSQL = schemaSQL
			}

			createSQL := tableStrategy.Generate(oplog, queryTracker)
			if createSQL != "" {
				result.CreateSQL = createSQL
			}

			result.AlterSQL = alterStrategy.Generate(oplog, queryTracker)

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
