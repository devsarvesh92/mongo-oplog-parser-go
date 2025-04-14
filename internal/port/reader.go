package port

import "github.com/devsarvesh92/mongoOplogParser/internal/domain/model"

type OplogReaderPort interface {
	ReadOplog() (model.Oplog, error)
}
