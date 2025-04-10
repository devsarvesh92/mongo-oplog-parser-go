package model

import "testing"

func TestOplog(t *testing.T) {
	tests := []struct {
		input        Oplog
		databaseName string
		tableName    string
		operation    string
	}{{
		input: Oplog{
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
		databaseName: "test",
		tableName:    "test.student",
		operation:    string(OpUpdate),
	}}

	for _, test := range tests {
		gotDBName, _ := test.input.GetDatabaseName()
		gotTableName, _ := test.input.GetTableName()
		gotOperation := test.input.GetOperationType()

		if gotDBName != test.databaseName {
			t.Errorf("Expected Database %v, got %v", test.databaseName, gotDBName)
		}

		if gotTableName != test.tableName {
			t.Errorf("Expected Table %v, got %v", test.tableName, gotTableName)
		}

		if gotOperation != OperationType(test.operation) {
			t.Errorf("Expected Operation %v, got %v", OperationType(test.operation), gotOperation)
		}
	}
}
