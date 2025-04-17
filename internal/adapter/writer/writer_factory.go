package writer

import (
	"log"

	"github.com/devsarvesh92/mongoOplogParser/internal/port"
)

type WriterType string

const (
	File     WriterType = "file"
	Database WriterType = "database"
)

func NewWriter(t WriterType, desitnation string) (writer port.SQLWriter, err error) {
	switch t {
	case File:
		writer, err = NewFileWriter(desitnation)
		if err != nil {
			log.Fatal("unable to initialize file writer %w", err)
		}

	case Database:
		writer, err = NewPostgresWriter(desitnation)
		if err != nil {
			log.Fatal("unable to connect to postgresql %w", err)
		}
	}
	return
}
