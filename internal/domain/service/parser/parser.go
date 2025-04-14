package parser

import (
	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
	"github.com/devsarvesh92/mongoOplogParser/internal/domain/service/strategy"
)

const (
	Float   = "FLOAT"
	VARCHAR = "VARCHAR(255)"
	BOOL    = "BOOLEAN"
)

type MongoOplogParser struct {
	InsertStrategy       *strategy.InsertStrategy
	UpdateStrategy       *strategy.UpdateStrategy
	DeleteStrategy       *strategy.DeleteStrategy
	NestedInsertStrategy *strategy.NestedInsertStratgey
}

// This will be moved out.
// Keeping it here for now
func NewMongoOplogParser() *MongoOplogParser {
	return &MongoOplogParser{
		InsertStrategy:       strategy.NewInsertStrategy(),
		DeleteStrategy:       strategy.NewDeleteStrategy(),
		UpdateStrategy:       strategy.NewUpdateStrategy(),
		NestedInsertStrategy: strategy.NewNestedInsertStragey(),
	}
}

// GenerateSQL transforms a set of MongoDB oplogs into SQL statements.
// It analyzes each oplog and generates the appropriate SQL commands including
// schema creation, table creation, inserts, updates, and deletes.
func (s *MongoOplogParser) GenerateSQL(oplogs []model.Oplog) (result model.Result) {
	if len(oplogs) == 0 {
		return
	}

	queryTracker := make(map[string]model.QueryTracker)

	for _, oplog := range oplogs {
		switch {
		case oplog.IsNestedDocument():
			result.SQL = append(result.SQL, s.NestedInsertStrategy.Generate(oplog, queryTracker)...)
		case oplog.IsInsert():
			result.SQL = append(result.SQL, s.InsertStrategy.Generate(oplog, queryTracker)...)
		case oplog.IsUpdate():
			updateSQL := s.UpdateStrategy.Generate(oplog, queryTracker)
			if updateSQL != "" {
				result.SQL = append(result.SQL, updateSQL)
			}
		case oplog.IsDelete():
			deleteSQL := s.DeleteStrategy.Generate(oplog, queryTracker)
			if deleteSQL != "" {
				result.SQL = append(result.SQL, deleteSQL)
			}
		}
		result.OperationType = string(oplog.GetOperationType())
	}
	return
}
