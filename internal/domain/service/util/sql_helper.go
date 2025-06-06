package util

import (
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
)

const (
	Float   = "FLOAT"
	VARCHAR = "VARCHAR(255)"
	BOOL    = "BOOLEAN"
)

var GenerateIDFunc = GenerateID

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

func GetSQLType(input interface{}) string {
	switch input.(type) {
	case int, int8, int16, float32, float64:
		return Float
	case bool:
		return BOOL
	default:
		return VARCHAR
	}
}

func GetConstraint(input string) string {
	if input == "_id" {
		return "PRIMARY KEY"
	} else {
		return ""
	}
}

func DiffCols(orgCols []string, newCols []string) (diff []string) {

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

func GenerateID() string {
	return uuid.New().String()
}
