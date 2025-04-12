package strategy

import (
	"fmt"
	"log"
	"strings"

	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
	"github.com/devsarvesh92/mongoOplogParser/internal/domain/service/util"
)

type DeleteStrategy struct{}

func NewDeleteStrategy() *DeleteStrategy {
	return &DeleteStrategy{}
}

func (s *DeleteStrategy) Generate(oplog model.Oplog, queryTracker map[string]model.QueryTracker) (result string) {
	var queryBuilder strings.Builder
	tableName, err := oplog.GetTableName()

	if err != nil {
		log.Printf("Error occured while extracting table name %v", err)
		return
	}

	queryBuilder.WriteString(fmt.Sprintf("DELETE FROM %v%v", tableName, util.BuildWhereClause(oplog.O)))
	deleteResult := queryBuilder.String()
	if _, ok := queryTracker[deleteResult]; !ok {
		result = deleteResult
		queryTracker[deleteResult] = model.QueryTracker{
			Type:  model.UPDATE,
			Query: result,
		}
	}
	return
}
