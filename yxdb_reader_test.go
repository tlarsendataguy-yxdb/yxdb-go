package yxdb_go_test

import (
	"fmt"
	yxdb_go "github.com/tlarsendataguy-yxdb/yxdb-go"
	"testing"
)

func TestGetReader(t *testing.T) {
	path := getPath(`AllNormalFields.yxdb`)
	yxdb, err := yxdb_go.ReadFile(path)
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	if yxdb.NumRecords() != 1 {
		t.Fatalf(`expected 1 record but got %v`, yxdb.NumRecords())
	}
	if fieldCount := len(yxdb.ListFields()); fieldCount != 16 {
		t.Fatalf(`expected 16 fields but got %v`, fieldCount)
	}

	read := 0
	for yxdb.Next() {
		if value, isNull := yxdb.ReadByteWithIndex(0); value != 1 || isNull {
			t.Fatalf(`expected 1 and isNull=false but got %v and isNull=%v`, value, isNull)
		}
		read++
	}
	if read != 1 {
		t.Fatalf(`expected 1 record read but got %v`, read)
	}
}

func getPath(fileName string) string {
	return fmt.Sprintf(`test_files/%v`, fileName)
}
