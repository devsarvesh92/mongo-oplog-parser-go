package oplog

import (
	"fmt"
	"sort"
	"strings"
)

const (
	OpInsert = "i"
	OpUpdate = "u"
	OpDelete = "d"
)

const (
	Float   = "FLOAT"
	VARCHAR = "VARCHAR(255)"
	BOOL    = "BOOLEAN"
)

type Oplog struct {
	Op string                 `"json:op"`
	Ns string                 `"json:ns"`
	O  map[string]interface{} `"json:o"`
	O2 map[string]interface{} `"json:o2"`
}

type Result struct {
	OperationType string
	SQL           []string
	SchemaSQL     string
	CreateSQL     string
	AlterSQL      []string
}

// GenerateSQL transforms a set of MongoDB oplogs into SQL statements.
// It analyzes each oplog and generates the appropriate SQL commands including
// schema creation, table creation, inserts, updates, and deletes.
func GenerateSQL(oplogs []Oplog) (result Result) {
	if len(oplogs) == 0 {
		return
	}

	var baseCols []string
	queryTracker := make(map[string]string)

	for id, oplog := range oplogs {
		columnNames := getCols(oplog)
		switch {
		case oplog.Op == OpInsert && id == 0:
			result.SchemaSQL = buildSchema(oplog, queryTracker)
			result.CreateSQL = buildTable(columnNames, oplog, queryTracker)
			result.SQL = append(result.SQL, buildInsert(columnNames, oplog, queryTracker))
			baseCols = columnNames
			result.OperationType = OpInsert

		case oplog.Op == OpInsert && id > 0:
			result.SQL = append(result.SQL, buildInsert(columnNames, oplog, queryTracker))
			diff := diffCols(baseCols, columnNames)
			for _, diffCol := range diff {
				alterQuery := buildAlter(diffCol, oplog, queryTracker)
				if alterQuery != "" {
					result.AlterSQL = append(result.AlterSQL, alterQuery)
				}
			}
			result.OperationType = OpInsert

		case oplog.Op == OpUpdate:
			result.SQL = append(result.SQL, buildUpdate(oplog))
			result.OperationType = OpUpdate
		case oplog.Op == OpDelete:
			result.SQL = append(result.SQL, buildDelete(oplog))
			result.OperationType = OpDelete
		}
	}
	return
}

func buildTable(columnNames []string, oplog Oplog, queryTracker map[string]string) (result string) {
	var tableSQL strings.Builder
	tableSQL.WriteString(fmt.Sprintf("CREATE TABLE %v ", oplog.Ns))
	tableSQL.WriteString("(")

	if _, ok := queryTracker[oplog.Ns]; !ok {
		for idx, col := range columnNames {
			tableSQL.WriteString(strings.TrimSpace(fmt.Sprintf("%v %v %v", col, getSQLType(oplog.O[col]), getConstraint(col))))
			if idx != len(columnNames)-1 {
				tableSQL.WriteString(", ")
			}
		}
		tableSQL.WriteString(");")
		result = tableSQL.String()
		queryTracker[oplog.Ns] = result
	}
	return
}

func buildInsert(columnNames []string, oplog Oplog, queryTracker map[string]string) (result string) {

	values := make([]string, 0)
	for _, col := range columnNames {
		values = append(values, formatColValue(oplog.O[col]))
	}
	insResult := fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v);", oplog.Ns, strings.Join(columnNames, ", "), strings.Join(values, ", "))

	if _, ok := queryTracker[insResult]; !ok {
		queryTracker[insResult] = insResult
		result = insResult
		return
	}
	return
}

func buildAlter(col string, oplog Oplog, queryTracker map[string]string) (result string) {
	alterResult := fmt.Sprintf("ALTER TABLE %v ADD %v %v;", oplog.Ns, col, getSQLType(formatColValue(oplog.O[col])))

	if _, ok := queryTracker[alterResult]; !ok {
		result = alterResult
		queryTracker[alterResult] = alterResult
	}
	return
}

func buildSchema(oplog Oplog, queryTracker map[string]string) (result string) {
	namespace := strings.Split(oplog.Ns, ".")[0]
	if _, ok := queryTracker[namespace]; !ok {
		result = fmt.Sprintf("CREATE SCHEMA %v;", namespace)
		queryTracker[namespace] = result
	}
	return
}

func buildUpdate(oplog Oplog) string {
	var query strings.Builder
	query.WriteString("UPDATE ")
	query.WriteString(oplog.Ns)
	query.WriteString(" SET")

	if diff, ok := oplog.O["diff"].(map[string]interface{}); ok {
		update, _ := diff["u"].(map[string]interface{})
		for col, val := range update {
			query.WriteString(fmt.Sprintf(" %v = %v", col, formatColValue(val)))
		}

		delete, _ := diff["d"].(map[string]interface{})
		for col, _ := range delete {
			query.WriteString(fmt.Sprintf(" %v = %v", col, "NULL"))
		}
	}

	query.WriteString(buildWhereClause(oplog.O2))
	return query.String()

}

func buildDelete(oplog Oplog) string {
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf("DELETE FROM %v%v", oplog.Ns, buildWhereClause(oplog.O)))
	return queryBuilder.String()
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

func getCols(oplogs Oplog) []string {
	columnNames := make([]string, 0)

	for col, _ := range oplogs.O {
		columnNames = append(columnNames, col)
	}

	sort.Strings(columnNames)
	return columnNames
}
