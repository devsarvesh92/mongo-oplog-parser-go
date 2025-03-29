package oplog

import (
	"fmt"
	"sort"
	"strings"
)

type Oplog struct {
	Op string                 `"json:op"`
	Ns string                 `"json:ns"`
	O  map[string]interface{} `"json:o"`
	O2 map[string]interface{} `"json:o2"`
}

func GenerateSQL(oplog Oplog) string {
	var result string
	switch oplog.Op {
	case "i":
		result = generateInsertStatement(oplog)

	case "u":
		result = generateUpdateStatement(oplog)

	case "d":
		result = generateDeleteStatement(oplog)
	}

	return result
}

func generateInsertStatement(oplog Oplog) string {
	columnNames := make([]string, 0)
	values := make([]string, 0)

	for col, _ := range oplog.O {
		columnNames = append(columnNames, col)
	}

	sort.Strings(columnNames)

	for _, col := range columnNames {
		values = append(values, formatColValue(oplog.O[col]))
	}

	return fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v);", oplog.Ns, strings.Join(columnNames, ", "), strings.Join(values, ", "))
}

func generateUpdateStatement(oplog Oplog) string {
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

func buildWhereClause(colValues map[string]interface{}) string {
	var whcl strings.Builder
	whcl.WriteString(" WHERE ")
	for col, val := range colValues {
		whcl.WriteString(fmt.Sprintf("%v = %v;", col, formatColValue(val)))
	}
	return whcl.String()
}

func generateDeleteStatement(oplog Oplog) string {
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf("DELETE FROM %v%v", oplog.Ns, buildWhereClause(oplog.O)))
	return queryBuilder.String()
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
