package strategy

import (
	"fmt"
	"strings"

	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
	"github.com/devsarvesh92/mongoOplogParser/internal/domain/service/util"
)

type InsertStrategy struct{}

func NewInsertStrategy() *InsertStrategy {
	return &InsertStrategy{}
}

func (s *InsertStrategy) Generate(oplog model.Oplog, queryTracker map[string]model.QueryTracker) (result string) {
	columnNames := util.GetCols(oplog.O)
	values := make([]string, 0)
	for _, col := range columnNames {
		values = append(values, util.FormatColValue(oplog.O[col]))
	}
	insResult := fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v);", oplog.Ns, strings.Join(columnNames, ", "), strings.Join(values, ", "))
	if _, ok := queryTracker[insResult]; !ok {
		result = insResult
		queryTracker[insResult] = model.QueryTracker{Type: model.INSERT, Query: result, Columns: columnNames}
		return
	}
	return

}
