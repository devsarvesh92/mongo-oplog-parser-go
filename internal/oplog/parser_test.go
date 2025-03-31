package oplog

import (
	"reflect"
	"testing"
)

func TestGenerateInsertStatement(t *testing.T) {
	tests := []struct {
		name     string
		oplog    Oplog
		expected Result
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
		expected: Result{
			OperationType: OpInsert,
			SQL:           []string{"INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);"},
			SchemaSQL:     "CREATE SCHEMA test;",
			TableSQL:      "CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);",
		},
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
		expected: Result{},
	},
	}

	for _, test := range tests {
		got := GenerateSQL(test.oplog)

		if !reflect.DeepEqual(got, test.expected) {
			t.Errorf("Expected %v Got %v", test.expected, got)
		}
	}
}

func TestGenerateUpdateStatement(t *testing.T) {
	tests := []struct {
		name     string
		oplog    Oplog
		expected Result
	}{
		{
			name: "Update statement",
			oplog: Oplog{
				Op: "u",
				Ns: "test.student",
				O: map[string]interface{}{
					"$v2": 2,
					"diff": map[string]interface{}{
						"u": map[string]interface{}{
							"is_graduated": true,
						},
					},
				},
				O2: map[string]interface{}{
					"_id": "635b79e231d82a8ab1de863b",
				},
			},
			expected: Result{
				OperationType: OpUpdate,
				SQL:           []string{"UPDATE test.student SET is_graduated = true WHERE _id = '635b79e231d82a8ab1de863b';"},
			},
		},
		{
			name: "Update statement",
			oplog: Oplog{
				Op: "u",
				Ns: "test.student",
				O: map[string]interface{}{
					"$v2": 2,
					"diff": map[string]interface{}{
						"d": map[string]interface{}{
							"roll_no": false,
						},
					},
				},
				O2: map[string]interface{}{
					"_id": "635b79e231d82a8ab1de863b",
				},
			},
			expected: Result{
				OperationType: OpUpdate,
				SQL:           []string{"UPDATE test.student SET roll_no = NULL WHERE _id = '635b79e231d82a8ab1de863b';"},
			},
		},
	}

	for _, test := range tests {
		got := GenerateSQL(test.oplog)

		if !reflect.DeepEqual(got, test.expected) {
			t.Errorf("Expected %v Got %v", test.expected, got)
		}
	}
}

func TestGenerateDeleteStatement(t *testing.T) {
	tests := []struct {
		name     string
		oplog    Oplog
		expected Result
	}{
		{
			name: "Delete statement",
			oplog: Oplog{
				Op: "d",
				Ns: "test.student",
				O: map[string]interface{}{
					"_id": "635b79e231d82a8ab1de863b",
				},
			},
			expected: Result{
				OperationType: OpDelete,
				SQL:           []string{"DELETE FROM test.student WHERE _id = '635b79e231d82a8ab1de863b';"},
			},
		},
	}

	for _, test := range tests {
		got := GenerateSQL(test.oplog)

		if !reflect.DeepEqual(got, test.expected) {
			t.Errorf("Expected %v Got %v", test.expected, got)
		}
	}
}
