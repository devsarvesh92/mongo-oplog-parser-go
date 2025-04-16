package port

type SQLWriter interface {
	WriteSQL(sql string) error
	Close()
}
