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
}

func GenerateSQL(oplong Oplog) string {
	var result string
	switch oplong.Op {
	case "i":
		result = generateInsertStatement(oplong)
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
