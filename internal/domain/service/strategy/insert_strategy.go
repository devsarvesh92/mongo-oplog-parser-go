package strategy

import (
	"fmt"
	"strings"

	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
	"github.com/devsarvesh92/mongoOplogParser/internal/domain/service/util"
)

type InsertStrategy struct {
	SchemaStrategy *SchemaStrategy
	TableStrategy  *TableStrategy
	AlterStrategy  *AlterStrategy
}

func NewInsertStrategy() *InsertStrategy {
	return &InsertStrategy{
		SchemaStrategy: NewSchemaStrategy(),
		TableStrategy:  NewTableStrategy(),
		AlterStrategy:  NewAlterStrategy(),
	}
}

func (s *InsertStrategy) Generate(oplog model.Oplog, queryTracker map[string]model.QueryTracker) (result []string) {
	schemaSQL := s.SchemaStrategy.Generate(oplog, queryTracker)
	if schemaSQL != "" {
		result = append(result, schemaSQL)
	}

	createSQL := s.TableStrategy.Generate(oplog, queryTracker)
	if createSQL != "" {
		result = append(result, createSQL)
	}

	result = append(result, s.AlterStrategy.Generate(oplog, queryTracker)...)

	columnNames := util.GetCols(oplog.O)
	values := make([]string, 0)
	for _, col := range columnNames {
		values = append(values, util.FormatColValue(oplog.O[col]))
	}
	insResult := fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v);", oplog.Ns, strings.Join(columnNames, ", "), strings.Join(values, ", "))
	if _, ok := queryTracker[insResult]; !ok {
		result = append(result, insResult)
		queryTracker[insResult] = model.QueryTracker{Type: model.INSERT, Query: insResult, Columns: columnNames}
		return
	}
	return

}
