package parser

import (
	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
	"github.com/devsarvesh92/mongoOplogParser/internal/domain/service/strategy"
	"github.com/devsarvesh92/mongoOplogParser/internal/domain/service/util"
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

	var baseCols []string
	queryTracker := make(map[string]model.QueryTracker)
	insertStrategy := strategy.NewInsertStrategy()
	updateStrategy := strategy.NewUpdateStrategy()
	deleteStrategy := strategy.NewDeleteStrategy()
	schemaStrategy := strategy.NewSchemaStrategy()
	tableStrategy := strategy.NewTableStrategy()
	alterStrategy := strategy.NewAlterStrategy()

	for id, oplog := range oplogs {
		columnNames := util.GetCols(oplog.O)
		switch {
		case oplog.IsInsert():
			if id == 0 {
				baseCols = columnNames
			}
			schemaSQL := schemaStrategy.Generate(oplog, queryTracker)
			createSQL := tableStrategy.Generate(oplog, queryTracker)
			result.SQL = append(result.SQL, insertStrategy.Generate(oplog, queryTracker))

			diff := util.DiffCols(baseCols, columnNames)
			for _, diffCol := range diff {
				alterQuery := alterStrategy.Generate(oplog, diffCol, queryTracker)
				if alterQuery != "" {
					result.AlterSQL = append(result.AlterSQL, alterQuery)
				}
			}
			if schemaSQL != "" {
				result.SchemaSQL = schemaSQL
			}

			if createSQL != "" {
				result.CreateSQL = createSQL
			}
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
