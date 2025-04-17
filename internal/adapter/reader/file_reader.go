package reader

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
)

type FileReader struct {
	file    *os.File
	decoder *json.Decoder
}

func NewFileReader(filePath string) (*FileReader, error) {
	file, err := os.Open(filePath)

	if err != nil {
		return nil, fmt.Errorf("unable to read file %w", err)
	}

	decoder := json.NewDecoder(file)

	if _, err := decoder.Token(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read opening array bracket: %w", err)
	}

	return &FileReader{
		file:    file,
		decoder: decoder,
	}, nil

}

func (r *FileReader) ReadOplog() (oplog model.Oplog, err error) {
	if !r.decoder.More() {
		return oplog, io.EOF
	}

	if err := r.decoder.Decode(&oplog); err != nil {
		return oplog, err
	}

	return oplog, nil
}

func (r *FileReader) ReadOplogs(ctx context.Context) <-chan model.Oplog {
	oplogChannel := make(chan model.Oplog)

	go func() {
		defer close(oplogChannel)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				oplog, err := r.ReadOplog()
				if err != nil {
					fmt.Println("error %w occured while reading oplog", err)
					return
				}
				oplogChannel <- oplog
			}
		}
	}()
	return oplogChannel
}

func (s *FileReader) Close() {
	s.file.Close()
}
