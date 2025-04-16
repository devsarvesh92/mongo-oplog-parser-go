package port

import "github.com/devsarvesh92/mongoOplogParser/internal/domain/model"

type OplogReader interface {
	ReadOplog() (model.Oplog, error)
	Close()
}
