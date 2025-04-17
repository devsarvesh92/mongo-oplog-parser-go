package writer

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type PostgresWriter struct {
	db *sql.DB
}

func NewPostgresWriter(connectionString string) (*PostgresWriter, error) {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to db %v", err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("unable to connect to ping db %v", err)
	}

	return &PostgresWriter{db: db}, nil
}

func (r *PostgresWriter) WriteSQL(sql string) error {
	_, err := r.db.Exec(sql)

	if err != nil {
		return fmt.Errorf("unable to execute SQL query: %w", err)
	}
	return nil
}

func (r *PostgresWriter) Close() {
	r.db.Close()
}
