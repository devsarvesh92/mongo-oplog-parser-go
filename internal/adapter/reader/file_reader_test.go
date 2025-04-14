package reader

import (
	"fmt"
	"io"
	"testing"

	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
)

func TestReadOplog(t *testing.T) {

	fileReader, error := NewFileReader("test_data/sample.json")
	var oplogs []model.Oplog
	if error != nil {
		t.Error("Unable to read file")
	}

	for {
		oplog, err := fileReader.ReadOplog()
		if err == io.EOF {
			fmt.Printf("File read sucessfully")
			break
		}

		if err != nil {
			t.Errorf("Unable to read record due to err %v", err)
			break
		}
		oplogs = append(oplogs, oplog)
	}

	if len(oplogs) != 16 {
		t.Errorf("Got %v,Expected %v", len(oplogs), 16)
	}

}
