package writer

import (
	"testing"
)

func TestWriteSQLToDB(t *testing.T) {
	connStr := "host=localhost port=5432 user=admin password=password dbname=mydb sslmode=disable"
	sqls := []string{"CREATE SCHEMA test;", "CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);"}
	tests := []struct {
		sqls    []string
		connStr string
	}{{
		sqls:    sqls,
		connStr: connStr,
	}}

	for _, test := range tests {
		writer, _ := NewPostgresWriter(test.connStr)

		for _, sql := range test.sqls {
			writer.WriteSQL(sql)
		}
		writer.Close()
	}

}
