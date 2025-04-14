package strategy

import (
	"fmt"
	"reflect"

	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
	"github.com/devsarvesh92/mongoOplogParser/internal/domain/service/util"
)

type NestedInsertStratgey struct {
	InsertStrategy *InsertStrategy
}

func NewNestedInsertStragey() *NestedInsertStratgey {
	return &NestedInsertStratgey{InsertStrategy: NewInsertStrategy()}
}

func (s *NestedInsertStratgey) Generate(oplog model.Oplog, queryTracker map[string]model.QueryTracker) (result []string) {
	oplogs := flatenOplog(oplog)

	for _, oplog := range oplogs {
		result = append(result, s.InsertStrategy.Generate(oplog, queryTracker)...)
	}
	return
}

func flatenOplog(oplog model.Oplog) (oplogs []model.Oplog) {
	// Identify nested properties
	// Build seperate oplog for it
	parent := model.Oplog{
		Op: string(model.OpInsert),
		Ns: oplog.Ns,
		O:  map[string]interface{}{},
	}
	shTableName, _ := oplog.GetShortTableName()
	flTableName, _ := oplog.GetFullTableName()
	childOplogs := make([]model.Oplog, 0)

	for key, value := range oplog.O {
		valueType := reflect.TypeOf(value)

		switch valueType.Kind() {
		case reflect.Map:
			vals := value.(map[string]interface{})
			vals["_id"] = util.GenerateIDFunc()
			vals[fmt.Sprintf("%v__id", shTableName)] = oplog.O["_id"]

			child := model.Oplog{
				Op: string(model.OpInsert),
				Ns: fmt.Sprintf("%v_%v", flTableName, key),
				O:  vals,
			}
			childOplogs = append(childOplogs, child)
		default:
			parent.O[key] = value
		}
	}

	oplogs = append(oplogs, parent)
	oplogs = append(oplogs, childOplogs...)

	return
}
