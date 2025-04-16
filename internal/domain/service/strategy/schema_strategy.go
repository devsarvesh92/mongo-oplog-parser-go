package strategy

import (
	"fmt"
	"log"

	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
)

type SchemaStrategy struct {
	Tracker *model.Tracker
}

func NewSchemaStrategy(tracker *model.Tracker) *SchemaStrategy {
	return &SchemaStrategy{
		Tracker: tracker,
	}
}

func (s *SchemaStrategy) Generate(oplog model.Oplog) (result string) {
	nameSpace, err := oplog.GetDatabaseName()
	if err != nil {
		log.Printf("Error occured while extracting schema name %v", err)
	}
	if _, ok := s.Tracker.Get(nameSpace); !ok {
		result = fmt.Sprintf("CREATE SCHEMA %v;", nameSpace)
		s.Tracker.Store(nameSpace, model.QueryTracker{
			Type:  model.CREATE_SCHEMA,
			Query: result,
		})
	}
	return
}
