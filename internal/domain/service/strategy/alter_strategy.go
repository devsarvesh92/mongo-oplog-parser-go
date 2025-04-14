package strategy

import (
	"fmt"

	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
	"github.com/devsarvesh92/mongoOplogParser/internal/domain/service/util"
)

type AlterStrategy struct{}

func NewAlterStrategy() *AlterStrategy {
	return &AlterStrategy{}
}

func (s *AlterStrategy) Generate(oplog model.Oplog, queryTracker map[string]model.QueryTracker) (result []string) {
	alteredCols := s.identifyAlteredCols(oplog, queryTracker)

	for _, col := range alteredCols {
		alterResult := fmt.Sprintf("ALTER TABLE %v ADD %v %v;", oplog.Ns, col, util.GetSQLType(util.FormatColValue(oplog.O[col])))

		if _, ok := queryTracker[alterResult]; !ok {
			result = append(result, alterResult)

			queryTracker[alterResult] = model.QueryTracker{
				Type:    model.ALTER_TABLE,
				Query:   alterResult,
				Columns: []string{col},
			}

		}
	}
	return
}

func (s *AlterStrategy) identifyAlteredCols(oplog model.Oplog, queryTracker map[string]model.QueryTracker) (altertedCols []string) {
	tableName, _ := oplog.GetFullTableName()
	tableQuery, ok := queryTracker[tableName]
	if !ok {
		return
	}

	cols := tableQuery.Columns

	if len(cols) < 1 {
		return
	}

	newCols := util.GetCols(oplog.O)
	altertedCols = util.DiffCols(cols, newCols)

	return
}
