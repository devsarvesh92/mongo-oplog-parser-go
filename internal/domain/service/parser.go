package service

import (
	"fmt"
	"log"
	"sort"
	"strings"

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

	var baseCols []string
	queryTracker := make(map[string]struct{})
	insertStrategy := strategy.NewInsertStrategy()
	updateStrategy := strategy.NewUpdateStrategy()

	for id, oplog := range oplogs {
		columnNames := getCols(oplog.O)
		namespace, err := oplog.GetDatabaseName()

		if err != nil {
			log.Printf("Error %v in extracting schema name", err)
			continue
		}

		tableName, err := oplog.GetTableName()

		if err != nil {
			log.Printf("Error %v in extracting table name", err)
			continue
		}

		switch {
		case oplog.IsInsert():
			if id == 0 {
				baseCols = columnNames
			}
			schemaSQL := buildSchema(oplog, namespace, queryTracker)
			createSQL := buildTable(columnNames, tableName, oplog.O, queryTracker)
			result.SQL = append(result.SQL, insertStrategy.Generate(oplog, queryTracker))

			diff := diffCols(baseCols, columnNames)
			for _, diffCol := range diff {
				alterQuery := buildAlter(diffCol, oplog, queryTracker)
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
			deleteSQL := buildDelete(oplog, tableName, queryTracker)
			if deleteSQL != "" {
				result.SQL = append(result.SQL, deleteSQL)
			}
		}
		result.OperationType = string(oplog.GetOperationType())
	}
	return
}

func buildTable(columnNames []string, tableName string, document map[string]interface{}, queryTracker map[string]struct{}) (result string) {
	var tableSQL strings.Builder
	tableSQL.WriteString(fmt.Sprintf("CREATE TABLE %v ", tableName))
	tableSQL.WriteString("(")

	if _, ok := queryTracker[tableName]; !ok {
		for idx, col := range columnNames {
			tableSQL.WriteString(strings.TrimSpace(fmt.Sprintf("%v %v %v", col, getSQLType(document[col]), getConstraint(col))))
			if idx != len(columnNames)-1 {
				tableSQL.WriteString(", ")
			}
		}
		tableSQL.WriteString(");")
		result = tableSQL.String()
		queryTracker[tableName] = struct{}{}
	}
	return
}

func buildAlter(col string, oplog model.Oplog, queryTracker map[string]struct{}) (result string) {
	alterResult := fmt.Sprintf("ALTER TABLE %v ADD %v %v;", oplog.Ns, col, getSQLType(formatColValue(oplog.O[col])))

	if _, ok := queryTracker[alterResult]; !ok {
		result = alterResult
		queryTracker[alterResult] = struct{}{}
	}
	return
}

func buildSchema(oplog model.Oplog, namespace string, queryTracker map[string]struct{}) (result string) {

	if _, ok := queryTracker[namespace]; !ok {
		result = fmt.Sprintf("CREATE SCHEMA %v;", namespace)
		queryTracker[namespace] = struct{}{}
	}
	return
}

func buildDelete(oplog model.Oplog, tableName string, queryTracker map[string]struct{}) (result string) {
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf("DELETE FROM %v%v", tableName, buildWhereClause(oplog.O)))
	deleteResult := queryBuilder.String()
	if _, ok := queryTracker[deleteResult]; !ok {
		result = deleteResult
		queryTracker[deleteResult] = struct{}{}
	}
	return
}

func buildWhereClause(colValues map[string]interface{}) string {
	var whcl strings.Builder
	whcl.WriteString(" WHERE ")
	for col, val := range colValues {
		whcl.WriteString(fmt.Sprintf("%v = %v;", col, formatColValue(val)))
	}
	return whcl.String()
}

func formatColValue(input interface{}) string {
	switch input.(type) {
	case int, int8, int16, float32, float64:
		return fmt.Sprintf("%v", input)
	case bool:
		return fmt.Sprintf("%t", input)
	default:
		return fmt.Sprintf("'%v'", input)
	}
}

func getSQLType(input interface{}) string {
	switch input.(type) {
	case int, int8, int16, float32, float64:
		return Float
	case bool:
		return BOOL
	default:
		return VARCHAR
	}
}

func getConstraint(input string) string {
	if input == "_id" {
		return "PRIMARY KEY"
	} else {
		return ""
	}
}

func diffCols(orgCols []string, newCols []string) (diff []string) {

	colMap := make(map[string]struct{})

	for _, col := range orgCols {
		colMap[col] = struct{}{}
	}

	for _, nc := range newCols {
		if _, ok := colMap[nc]; !ok {
			diff = append(diff, nc)
		}
	}
	return diff
}

func getCols(document map[string]interface{}) []string {
	columnNames := make([]string, 0)

	for col, _ := range document {
		columnNames = append(columnNames, col)
	}

	sort.Strings(columnNames)
	return columnNames
}
