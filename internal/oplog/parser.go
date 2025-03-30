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
	SQL           string
	SchemaSQL     string
	TableSQL      string
}

func GenerateSQL(oplog Oplog) Result {
	var result Result
	switch oplog.Op {
	case OpInsert:
		result = generateInsertStatement(oplog)

	case OpUpdate:
		result = generateUpdateStatement(oplog)

	case OpDelete:
		result = generateDeleteStatement(oplog)
	}

	return result
}

func generateInsertStatement(oplog Oplog) Result {

	columnNames := make([]string, 0)
	values := make([]string, 0)

	for col, _ := range oplog.O {
		columnNames = append(columnNames, col)
	}

	sort.Strings(columnNames)

	// Build Table SQL
	var tableSQL strings.Builder
	tableSQL.WriteString(fmt.Sprintf("CREATE TABLE %v ", oplog.Ns))
	tableSQL.WriteString("(")

	for idx, col := range columnNames {
		values = append(values, formatColValue(oplog.O[col]))
		tableSQL.WriteString(strings.TrimSpace(fmt.Sprintf("%v %v %v", col, getSQLType(oplog.O[col]), getConstraint(col))))
		if idx != len(columnNames)-1 {
			tableSQL.WriteString(", ")
		}

	}

	tableSQL.WriteString(");")

	namespace := strings.Split(oplog.Ns, ".")[0]

	return Result{
		OperationType: OpInsert,
		SQL:           fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v);", oplog.Ns, strings.Join(columnNames, ", "), strings.Join(values, ", ")),
		SchemaSQL:     fmt.Sprintf("CREATE SCHEMA %v;", namespace),
		TableSQL:      tableSQL.String(),
	}
}

func generateUpdateStatement(oplog Oplog) Result {
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
	return Result{
		OperationType: OpUpdate,
		SQL:           query.String(),
	}

}

func generateDeleteStatement(oplog Oplog) Result {
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf("DELETE FROM %v%v", oplog.Ns, buildWhereClause(oplog.O)))
	return Result{
		OperationType: OpDelete,
		SQL:           queryBuilder.String(),
	}
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
