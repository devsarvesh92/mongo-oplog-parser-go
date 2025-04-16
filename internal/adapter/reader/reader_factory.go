package reader

import (
	"log"

	"github.com/devsarvesh92/mongoOplogParser/internal/port"
)

type ReaderType string

const (
	MongoFile   ReaderType = "mongo-file"
	MongoStream ReaderType = "mongo-stream"
)

func NewReader(t ReaderType, source string) (reader port.OplogReader, err error) {
	switch t {
	case MongoFile:
		reader, err := NewFileReader(source)
		if err != nil {
			log.Fatal("unable to initialize file reader %w", err)
		}
		return reader, nil

	case MongoStream:
		reader, err := NewMongoReader(source)
		if err != nil {
			log.Fatal("unable to connect to db %w", err)
		}
		return reader, nil
	}
	return
}
