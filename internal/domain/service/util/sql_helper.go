package util

import (
	"fmt"
	"sort"
	"strings"
)

func FormatColValue(input interface{}) string {
	switch input.(type) {
	case int, int8, int16, float32, float64:
		return fmt.Sprintf("%v", input)
	case bool:
		return fmt.Sprintf("%t", input)
	default:
		return fmt.Sprintf("'%v'", input)
	}
}

func GetCols(document map[string]interface{}) []string {
	columnNames := make([]string, 0)

	for col, _ := range document {
		columnNames = append(columnNames, col)
	}

	sort.Strings(columnNames)
	return columnNames
}

func BuildWhereClause(colValues map[string]interface{}) string {
	var whcl strings.Builder
	whcl.WriteString(" WHERE ")
	for col, val := range colValues {
		whcl.WriteString(fmt.Sprintf("%v = %v;", col, FormatColValue(val)))
	}
	return whcl.String()
}
