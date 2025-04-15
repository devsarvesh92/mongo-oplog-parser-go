package port

type FileWriterPort interface {
	WriteSQL(sql string) error
}
