package model

import "testing"

func TestOplog(t *testing.T) {
	tests := []struct {
		input        Oplog
		databaseName string
		tableName    string
		operation    string
		isNested     bool
	}{{
		input: Oplog{
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
		databaseName: "test",
		tableName:    "test.student",
		operation:    string(OpInsert),
		isNested:     false,
	}, {
		input: Oplog{
			Op: "i",
			Ns: "test.student",
			O: map[string]interface{}{
				"_id":           "635b79e231d82a8ab1de863b",
				"name":          "Selena Miller",
				"roll_no":       51,
				"is_graduated":  false,
				"date_of_birth": "2000-01-30",
				"phone": map[string]interface{}{
					"personal": "7678456640",
					"work":     "8130097989",
				},
			},
		},
		databaseName: "test",
		tableName:    "test.student",
		operation:    string(OpInsert),
		isNested:     true,
	},
	}

	for _, test := range tests {
		gotDBName, _ := test.input.GetDatabaseName()
		gotTableName, _ := test.input.GetTableName()
		gotOperation := test.input.GetOperationType()
		isNestedDocument := test.input.IsNestedDocument()

		if gotDBName != test.databaseName {
			t.Errorf("Expected Database %v, got %v", test.databaseName, gotDBName)
		}

		if gotTableName != test.tableName {
			t.Errorf("Expected Table %v, got %v", test.tableName, gotTableName)
		}

		if gotOperation != OperationType(test.operation) {
			t.Errorf("Expected Operation %v, got %v", OperationType(test.operation), gotOperation)
		}

		if test.isNested != isNestedDocument {
			t.Errorf("Expected %v, got %v", test.isNested, isNestedDocument)
		}
	}
}
