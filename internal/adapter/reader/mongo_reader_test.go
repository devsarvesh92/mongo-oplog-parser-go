package reader

import (
	"fmt"
	"testing"
)

func TestReadMongoOplog(t *testing.T) {
	mongoReader, err := NewMongoReader("mongodb://localhost:27017")

	if err != nil {
		t.Errorf("error while connection mongo db %v", err)
	}
	defer mongoReader.Close()
	got, err := mongoReader.ReadOplog()
	if err != nil {
		t.Errorf("error while reading oplog %v", err)
	}
	fmt.Print(got)
}
