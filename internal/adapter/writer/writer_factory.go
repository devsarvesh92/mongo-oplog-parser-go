package writer

import (
	"fmt"
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
		reader, err := NewFileWriter(desitnation)
		if err != nil {
			log.Fatal("unable to initialize file reader %w", err)
		}
		return reader, nil

	case Database:
		//
		return nil, fmt.Errorf("database writer not implemented yet")
	}
	return
}
