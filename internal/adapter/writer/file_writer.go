package writer

import (
	"fmt"
	"os"
	"strings"
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

	if !strings.HasSuffix(sql, ";") {
		sql += ";"
	}

	if !strings.HasSuffix(sql, "\n") {
		sql += "\n"
	}

	_, err := s.file.WriteString(sql)
	if err != nil {
		return fmt.Errorf("Error occured while writing to file %w", err)
	}

	return nil
}

func (s *FileWriter) Close() error {
	err := s.file.Close()

	if err != nil {
		fmt.Errorf("Error occured while closing file %w", err)
	}

	return nil
}
