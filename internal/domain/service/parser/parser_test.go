package parser

import (
	"reflect"
	"testing"

	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
	"github.com/devsarvesh92/mongoOplogParser/internal/domain/service/util"
)

func TestGenerateInsertStatement(t *testing.T) {
	mongoOplogParser := getMongoOplogParser()

	tests := []struct {
		name     string
		oplog    model.Oplog
		expected model.Result
	}{{
		name: "parsing insert oplog",
		oplog: model.Oplog{
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
		expected: model.Result{
			OperationType: string(model.OpInsert),
			SQL:           []string{"CREATE SCHEMA test;", "CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);", "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);"},
		},
	},
	}

	for _, test := range tests {
		got := mongoOplogParser.GenerateSQL([]model.Oplog{test.oplog})

		if !reflect.DeepEqual(got, test.expected) {
			t.Errorf("Expected %v Got %v", test.expected, got)
		}
	}
}

func TestGenerateUpdateStatement(t *testing.T) {
	mongoOplogParser := getMongoOplogParser()
	tests := []struct {
		name     string
		oplog    model.Oplog
		expected model.Result
	}{
		{
			name: "Update statement",
			oplog: model.Oplog{
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
			expected: model.Result{
				OperationType: string(model.OpUpdate),
				SQL:           []string{"UPDATE test.student SET is_graduated = true WHERE _id = '635b79e231d82a8ab1de863b';"},
			},
		},
		{
			name: "Update statement",
			oplog: model.Oplog{
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
			expected: model.Result{
				OperationType: string(model.OpUpdate),
				SQL:           []string{"UPDATE test.student SET roll_no = NULL WHERE _id = '635b79e231d82a8ab1de863b';"},
			},
		},
	}

	for _, test := range tests {
		got := mongoOplogParser.GenerateSQL([]model.Oplog{test.oplog})

		if !reflect.DeepEqual(got, test.expected) {
			t.Errorf("Expected %v Got %v", test.expected, got)
		}
	}
}

func TestGenerateDeleteStatement(t *testing.T) {
	mongoOplogParser := getMongoOplogParser()
	tests := []struct {
		name     string
		oplog    model.Oplog
		expected model.Result
	}{
		{
			name: "Delete statement",
			oplog: model.Oplog{
				Op: "d",
				Ns: "test.student",
				O: map[string]interface{}{
					"_id": "635b79e231d82a8ab1de863b",
				},
			},
			expected: model.Result{
				OperationType: string(model.OpDelete),
				SQL:           []string{"DELETE FROM test.student WHERE _id = '635b79e231d82a8ab1de863b';"},
			},
		},
	}

	for _, test := range tests {
		got := mongoOplogParser.GenerateSQL([]model.Oplog{test.oplog})

		if !reflect.DeepEqual(got, test.expected) {
			t.Errorf("Expected %v Got %v", test.expected, got)
		}
	}
}

func TestMultipleOplogs(t *testing.T) {
	mongoOplogParser := getMongoOplogParser()
	tests := []struct {
		name     string
		oplogs   []model.Oplog
		expected model.Result
	}{{
		name: "parsing insert oplog",
		oplogs: []model.Oplog{
			{
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
			{
				Op: "i",
				Ns: "test.student",
				O: map[string]interface{}{
					"_id":           "14798c213f273a7ca2cf5174",
					"name":          "George Smith",
					"roll_no":       21,
					"is_graduated":  true,
					"date_of_birth": "2001-03-23",
				},
			},
		},
		expected: model.Result{
			OperationType: string(model.OpInsert),
			SQL:           []string{"CREATE SCHEMA test;", "CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);", "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);", "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('14798c213f273a7ca2cf5174', '2001-03-23', true, 'George Smith', 21);"},
		},
	},
	}

	for _, test := range tests {
		got := mongoOplogParser.GenerateSQL(test.oplogs)

		if !reflect.DeepEqual(got, test.expected) {
			t.Errorf("Expected %v Got %v", test.expected, got)
		}
	}
}

func TestAlterTableWithMultipleOplogs(t *testing.T) {
	mongoOplogParser := getMongoOplogParser()
	tests := []struct {
		name     string
		oplogs   []model.Oplog
		expected model.Result
	}{{
		name: "parsing insert oplog",
		oplogs: []model.Oplog{
			{
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
			{
				Op: "i",
				Ns: "test.student",
				O: map[string]interface{}{
					"_id":           "14798c213f273a7ca2cf5174",
					"name":          "George Smith",
					"roll_no":       21,
					"is_graduated":  true,
					"date_of_birth": "2001-03-23",
					"phone":         "+91-81254966457",
				},
			},
		},
		expected: model.Result{
			OperationType: string(model.OpInsert),
			SQL:           []string{"CREATE SCHEMA test;", "CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);", "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);", "ALTER TABLE test.student ADD phone VARCHAR(255);", "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, phone, roll_no) VALUES ('14798c213f273a7ca2cf5174', '2001-03-23', true, 'George Smith', '+91-81254966457', 21);"},
		},
	},
	}
	for _, test := range tests {
		got := mongoOplogParser.GenerateSQL(test.oplogs)

		if !reflect.DeepEqual(got, test.expected) {
			t.Errorf("Expected %v Got %v", test.expected, got)
		}
	}
}

func TestNestedOplogs(t *testing.T) {
	mongoOplogParser := getMongoOplogParser()
	originalGenerateID := util.GenerateIDFunc
	util.GenerateIDFunc = func() string {
		return "14798c213f273a7ca2cf5199"
	}
	defer func() {
		util.GenerateIDFunc = originalGenerateID
	}()

	tests := []struct {
		name     string
		oplogs   []model.Oplog
		expected model.Result
	}{{name: "Nested oplog", oplogs: []model.Oplog{model.Oplog{
		Op: string(model.OpInsert),
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
			"address": []map[string]interface{}{
				{
					"line1": "481 Harborsburgh",
					"zip":   "89799",
				},
				{
					"line1": "329 Flatside",
					"zip":   "80872",
				},
			},
		},
	}}, expected: model.Result{
		OperationType: string(model.OpInsert),
		SQL: []string{
			"CREATE SCHEMA test;",
			"CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);",
			"INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);",
			"CREATE TABLE test.student_address (_id VARCHAR(255) PRIMARY KEY, line1 VARCHAR(255), student__id VARCHAR(255), zip VARCHAR(255));",
			"INSERT INTO test.student_address (_id, line1, student__id, zip) VALUES ('14798c213f273a7ca2cf5199', '481 Harborsburgh', '635b79e231d82a8ab1de863b', '89799');",
			"INSERT INTO test.student_address (_id, line1, student__id, zip) VALUES ('14798c213f273a7ca2cf5199', '329 Flatside', '635b79e231d82a8ab1de863b', '80872');",
			"CREATE TABLE test.student_phone (_id VARCHAR(255) PRIMARY KEY, personal VARCHAR(255), student__id VARCHAR(255), work VARCHAR(255));",
			"INSERT INTO test.student_phone (_id, personal, student__id, work) VALUES ('14798c213f273a7ca2cf5199', '7678456640', '635b79e231d82a8ab1de863b', '8130097989');",
		},
	}}}

	for _, test := range tests {
		result := mongoOplogParser.GenerateSQL(test.oplogs)
		got := result.SQL

		if !reflect.DeepEqual(got, test.expected.SQL) {
			t.Errorf("Expected %v Got %v", test.expected, got)
		}
	}
}

func getMongoOplogParser() MongoOplogParser {
	return *NewMongoOplogParser()
}
