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
	oplogs := s.flatenOplog(oplog)

	for _, oplog := range oplogs {
		result = append(result, s.InsertStrategy.Generate(oplog, queryTracker)...)
	}
	return
}

func (s *NestedInsertStratgey) flatenOplog(oplog model.Oplog) (oplogs []model.Oplog) {
	// Identify nested properties
	// Build seperate oplog for it
	columnNames := util.GetCols(oplog.O)

	parent := model.Oplog{
		Op: string(model.OpInsert),
		Ns: oplog.Ns,
		O:  map[string]interface{}{},
	}
	childOplogs := make([]model.Oplog, 0)

	for _, col := range columnNames {
		value := oplog.O[col]
		valueType := reflect.TypeOf(value)
		switch valueType.Kind() {
		case reflect.Map:
			child := s.parseMapNode(value.(map[string]interface{}), oplog, col)
			childOplogs = append(childOplogs, child)
		case reflect.Array | reflect.Slice:
			children := s.parseListNode(value.([]interface{}), oplog, col)
			childOplogs = append(childOplogs, children...)
		default:
			parent.O[col] = value
		}
	}

	oplogs = append(oplogs, parent)
	oplogs = append(oplogs, childOplogs...)

	return
}

func (s *NestedInsertStratgey) parseMapNode(doc map[string]interface{}, parent model.Oplog, tableName string) (oplog model.Oplog) {

	shTableName, _ := parent.GetShortTableName()
	dbName, _ := parent.GetDatabaseName()

	doc["_id"] = util.GenerateIDFunc()
	doc[fmt.Sprintf("%v__id", shTableName)] = parent.O["_id"]

	return model.Oplog{
		Op: string(model.OpInsert),
		Ns: fmt.Sprintf("%v.%v_%v", dbName, shTableName, tableName),
		O:  doc,
	}
}

func (s *NestedInsertStratgey) parseListNode(docs []interface{}, parent model.Oplog, tableName string) (oplogs []model.Oplog) {

	for _, doc := range docs {
		oplogs = append(oplogs, s.parseMapNode(doc.(map[string]interface{}), parent, tableName))
	}
	return
}
