package strategy

import (
	"fmt"
	"log"
	"strings"

	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
	"github.com/devsarvesh92/mongoOplogParser/internal/domain/service/util"
)

type DeleteStrategy struct {
	Tracker *model.Tracker
}

func NewDeleteStrategy(tracker *model.Tracker) *DeleteStrategy {
	return &DeleteStrategy{Tracker: tracker}
}

func (s *DeleteStrategy) Generate(oplog model.Oplog) (result string) {
	var queryBuilder strings.Builder
	tableName, err := oplog.GetFullTableName()

	if err != nil {
		log.Printf("Error occured while extracting table name %v", err)
		return
	}

	queryBuilder.WriteString(fmt.Sprintf("DELETE FROM %v%v", tableName, util.BuildWhereClause(oplog.O)))
	deleteResult := queryBuilder.String()
	if _, ok := s.Tracker.Get(deleteResult); !ok {
		result = deleteResult
		s.Tracker.Store(deleteResult, model.QueryTracker{
			Type:  model.UPDATE,
			Query: result,
		})
	}
	return
}
