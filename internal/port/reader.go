package port

import (
	"context"

	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
)

type OplogReader interface {
	ReadOplog() (model.Oplog, error)
	ReadOplogs(ctx context.Context) <-chan model.Oplog
	Close()
}
