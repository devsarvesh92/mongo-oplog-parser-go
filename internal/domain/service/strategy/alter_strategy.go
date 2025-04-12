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

func (s *AlterStrategy) Generate(oplog model.Oplog, col string, queryTracker map[string]struct{}) (result string) {
	alterResult := fmt.Sprintf("ALTER TABLE %v ADD %v %v;", oplog.Ns, col, util.GetSQLType(util.FormatColValue(oplog.O[col])))

	if _, ok := queryTracker[alterResult]; !ok {
		result = alterResult
		queryTracker[alterResult] = struct{}{}
	}
	return
}
