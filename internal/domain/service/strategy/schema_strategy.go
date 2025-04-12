package strategy

import (
	"fmt"
	"log"

	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
)

type SchemaStrategy struct{}

func NewSchemaStrategy() *SchemaStrategy {
	return &SchemaStrategy{}
}

func (s *SchemaStrategy) Generate(oplog model.Oplog, queryTracker map[string]struct{}) (result string) {
	nameSpace, err := oplog.GetDatabaseName()
	if err != nil {
		log.Printf("Error occured while extracting schema name %v", err)
	}
	if _, ok := queryTracker[nameSpace]; !ok {
		result = fmt.Sprintf("CREATE SCHEMA %v;", nameSpace)
		queryTracker[nameSpace] = struct{}{}
	}
	return
}
