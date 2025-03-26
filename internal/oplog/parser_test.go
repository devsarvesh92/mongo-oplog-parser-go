package oplog

import (
	"testing"
)

func TestGenerateSQL(t *testing.T) {
	tests := []struct {
		name     string
		oplog    Oplog
		expected string
	}{{
		name: "parsing insert oplog",
		oplog: Oplog{
			Op: "i",
			Ns: "test.student",
			O: map[string]interface{}{
				"_id":           "635b79e231d82a8ab1de863b",
				"name":          "Selena Miller",
				"roll_no":       51,
				"is_graduated":  false,
				"date_of_birth": "2000-01-30",
			},
		},
		expected: "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);",
	}, {
		name: "parsing invalid input",
		oplog: Oplog{
			Op: "k",
			Ns: "test.student",
			O: map[string]interface{}{
				"_id":           "635b79e231d82a8ab1de863b",
				"name":          "Selena Miller",
				"roll_no":       51,
				"is_graduated":  false,
				"date_of_birth": "2000-01-30",
			},
		},
		expected: "",
	}}

	for _, test := range tests {
		got := GenerateSQL(test.oplog)

		if got != test.expected {
			t.Errorf("Expected %v Got %v", test.expected, got)
		}
	}
}
