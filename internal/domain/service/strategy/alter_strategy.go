package strategy

import (
	"fmt"

	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
	"github.com/devsarvesh92/mongoOplogParser/internal/domain/service/util"
)

type AlterStrategy struct {
	Tracker *model.Tracker
}

func NewAlterStrategy(tracker *model.Tracker) *AlterStrategy {
	return &AlterStrategy{Tracker: tracker}
}

func (s *AlterStrategy) Generate(oplog model.Oplog) (result []string) {
	alteredCols := s.identifyAlteredCols(oplog)

	for _, col := range alteredCols {
		alterResult := fmt.Sprintf("ALTER TABLE %v ADD %v %v;", oplog.Ns, col, util.GetSQLType(util.FormatColValue(oplog.O[col])))

		if _, ok := s.Tracker.Get(alterResult); !ok {
			result = append(result, alterResult)

			s.Tracker.Store(alterResult, model.QueryTracker{
				Type:    model.ALTER_TABLE,
				Query:   alterResult,
				Columns: []string{col},
			})

		}
	}
	return
}

func (s *AlterStrategy) identifyAlteredCols(oplog model.Oplog) (altertedCols []string) {
	tableName, _ := oplog.GetFullTableName()
	tableQuery, ok := s.Tracker.Get(tableName)
	if !ok {
		return
	}

	cols := tableQuery.(model.QueryTracker).Columns

	if len(cols) < 1 {
		return
	}

	newCols := util.GetCols(oplog.O)
	altertedCols = util.DiffCols(cols, newCols)

	return
}
