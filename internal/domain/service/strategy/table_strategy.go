package strategy

import (
	"fmt"
	"log"
	"strings"

	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
	"github.com/devsarvesh92/mongoOplogParser/internal/domain/service/util"
)

type TableStrategy struct{ Tracker *model.Tracker }

func NewTableStrategy(tracker *model.Tracker) *TableStrategy {
	return &TableStrategy{
		Tracker: tracker,
	}
}

func (s *TableStrategy) Generate(oplog model.Oplog) (result string) {
	var tableSQL strings.Builder
	tableName, err := oplog.GetFullTableName()
	if err != nil {
		log.Printf("Error occured while extracting table name %v", err)
	}

	columnNames := util.GetCols(oplog.O)

	tableSQL.WriteString(fmt.Sprintf("CREATE TABLE %v ", tableName))
	tableSQL.WriteString("(")

	if _, ok := s.Tracker.Get(tableName); !ok {
		for idx, col := range columnNames {
			tableSQL.WriteString(strings.TrimSpace(fmt.Sprintf("%v %v %v", col, util.GetSQLType(oplog.O[col]), util.GetConstraint(col))))
			if idx != len(columnNames)-1 {
				tableSQL.WriteString(", ")
			}
		}
		tableSQL.WriteString(");")
		result = tableSQL.String()

		s.Tracker.Store(tableName, model.QueryTracker{
			Type:    model.CREATE_TABLE,
			Query:   result,
			Columns: columnNames,
		})

	}
	return
}
