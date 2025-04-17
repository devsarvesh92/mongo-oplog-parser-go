package writer

import (
	"fmt"
	"os"
)

type FileWriter struct {
	file     *os.File
	filePath string
}

func NewFileWriter(filePath string) (*FileWriter, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("error occured while creating file %w", err)
	}

	return &FileWriter{
		file:     file,
		filePath: filePath,
	}, nil
}

func (s *FileWriter) WriteSQL(sql string) error {
	if sql == "" {
		return fmt.Errorf("invalid sql")
	}

	_, err := s.file.WriteString(sql)
	if err != nil {
		return fmt.Errorf("Error occured while writing to file %w", err)
	}

	return nil
}

func (s *FileWriter) Close() {
	s.file.Close()
}
