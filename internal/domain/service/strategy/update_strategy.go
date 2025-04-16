package strategy

import (
	"fmt"
	"log"
	"strings"

	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
	"github.com/devsarvesh92/mongoOplogParser/internal/domain/service/util"
)

type UpdateStrategy struct {
	Tracker *model.Tracker
}

func NewUpdateStrategy(tracker *model.Tracker) *UpdateStrategy {
	return &UpdateStrategy{tracker}
}

func (s *UpdateStrategy) Generate(oplog model.Oplog) (result string) {
	var query strings.Builder
	tableName, err := oplog.GetFullTableName()

	if err != nil {
		log.Printf("Error occured while extracting table name %v", err)
		return
	}

	query.WriteString("UPDATE ")
	query.WriteString(tableName)
	query.WriteString(" SET")

	if diff, ok := oplog.O["diff"].(map[string]interface{}); ok {
		update, _ := diff["u"].(map[string]interface{})
		for col, val := range update {
			query.WriteString(fmt.Sprintf(" %v = %v", col, util.FormatColValue(val)))
		}

		delete, _ := diff["d"].(map[string]interface{})
		for col, _ := range delete {
			query.WriteString(fmt.Sprintf(" %v = %v", col, "NULL"))
		}
	}

	query.WriteString(util.BuildWhereClause(oplog.O2))

	updateResult := query.String()

	if _, ok := s.Tracker.Get(updateResult); !ok {
		result = updateResult
		s.Tracker.Store(updateResult, model.QueryTracker{
			Type:  model.UPDATE,
			Query: result,
		})
	}

	return

}
