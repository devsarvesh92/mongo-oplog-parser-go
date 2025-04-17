package writer

import (
	"testing"
)

func TestWriteSQL(t *testing.T) {
	opFile := "/tmp/test.txt"
	sqls := []string{"CREATE SCHEMA test; \n", "CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT); \n"}
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

}
