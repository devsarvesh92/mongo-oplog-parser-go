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
	Tracker        *model.Tracker
}

func NewInsertStrategy(tracker *model.Tracker) *InsertStrategy {
	return &InsertStrategy{
		SchemaStrategy: NewSchemaStrategy(tracker),
		TableStrategy:  NewTableStrategy(tracker),
		AlterStrategy:  NewAlterStrategy(tracker),
		Tracker:        tracker,
	}
}

func (s *InsertStrategy) Generate(oplog model.Oplog) (result []string) {
	schemaSQL := s.SchemaStrategy.Generate(oplog)
	if schemaSQL != "" {
		result = append(result, schemaSQL)
	}

	createSQL := s.TableStrategy.Generate(oplog)
	if createSQL != "" {
		result = append(result, createSQL)
	}

	result = append(result, s.AlterStrategy.Generate(oplog)...)

	columnNames := util.GetCols(oplog.O)
	values := make([]string, 0)
	for _, col := range columnNames {
		values = append(values, util.FormatColValue(oplog.O[col]))
	}
	insResult := fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v);", oplog.Ns, strings.Join(columnNames, ", "), strings.Join(values, ", "))
	if _, ok := s.Tracker.Get(insResult); !ok {
		result = append(result, insResult)
		s.Tracker.Store(insResult, model.QueryTracker{Type: model.INSERT, Query: insResult, Columns: columnNames})
		return
	}
	return

}
