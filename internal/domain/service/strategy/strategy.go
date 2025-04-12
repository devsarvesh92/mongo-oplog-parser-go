package strategy

import "github.com/devsarvesh92/mongoOplogParser/internal/domain/model"

type SQLGenerationStragey interface {
	Generate(oplog model.Oplog, queryTracker map[string]struct{}) string
}
