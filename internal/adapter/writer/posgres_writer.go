package writer

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

type PostgresWriter struct {
	db *sql.DB
}

func NewPostgresWriter(connectionString string) (*PostgresWriter, error) {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatalf("unable to connect to db %v", err)
	}

	if err = db.Ping(); err != nil {
		log.Fatalf("unable to connect to ping db %v", err)
	}
	fmt.Println("Successfully connected to PostgreSQL!")

	return &PostgresWriter{db: db}, nil
}

func (r *PostgresWriter) WriteSQL(sql string) error {
	_, err := r.db.Exec(sql)

	if err != nil {
		return fmt.Errorf("unable to execute %v because of %v", sql, err)
	}
	return nil
}

func (r *PostgresWriter) Close() {
	r.db.Close()
}
