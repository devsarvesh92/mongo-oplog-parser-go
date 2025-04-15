package writer

import (
	"bufio"
	"os"
	"reflect"
	"testing"
)

func TestWriteSQL(t *testing.T) {
	opFile := "/tmp/test.txt"
	sqls := []string{"CREATE SCHEMA test;", "CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);"}
	tests := []struct {
		sqls           []string
		outputFilePath string
	}{{
		sqls:           sqls,
		outputFilePath: opFile,
	}}

	for _, test := range tests {
		fileWriter, err := NewFileWriter(test.outputFilePath)
		if err != nil {
			t.Errorf("Unable to create file writer %v", err)
		}
		for _, sql := range test.sqls {
			fileWriter.WriteSQL(sql)
		}
		fileWriter.Close()
	}

	// Assert written sqls

	file, _ := os.Open(opFile)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lines []string

	// Read line by line
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if !reflect.DeepEqual(sqls, lines) {
		t.Errorf("Expected %v, Got %v", sqls, lines)
	}
}
