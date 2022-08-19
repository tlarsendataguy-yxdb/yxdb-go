package yxdb_go_test

import (
	"fmt"
	yx "github.com/tlarsendataguy-yxdb/yxdb"
	"os"
	"strings"
	"testing"
	"time"
)

func TestGetReader(t *testing.T) {
	path := getPath(`AllNormalFields.yxdb`)
	yxdb, err := yx.ReadFile(path)

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
		checkField(t, byte(1), false, func() (interface{}, bool) { return yxdb.ReadByteWithIndex(0) })
		checkField(t, byte(1), false, func() (interface{}, bool) { return yxdb.ReadByteWithName(`ByteField`) })
		checkField(t, true, false, func() (interface{}, bool) { return yxdb.ReadBoolWithIndex(1) })
		checkField(t, true, false, func() (interface{}, bool) { return yxdb.ReadBoolWithName(`BoolField`) })
		checkField(t, int64(16), false, func() (interface{}, bool) { return yxdb.ReadInt64WithIndex(2) })
		checkField(t, int64(16), false, func() (interface{}, bool) { return yxdb.ReadInt64WithName(`Int16Field`) })
		checkField(t, int64(32), false, func() (interface{}, bool) { return yxdb.ReadInt64WithIndex(3) })
		checkField(t, int64(32), false, func() (interface{}, bool) { return yxdb.ReadInt64WithName(`Int32Field`) })
		checkField(t, int64(64), false, func() (interface{}, bool) { return yxdb.ReadInt64WithIndex(4) })
		checkField(t, int64(64), false, func() (interface{}, bool) { return yxdb.ReadInt64WithName(`Int64Field`) })
		checkField(t, 123.45, false, func() (interface{}, bool) { return yxdb.ReadFloat64WithIndex(5) })
		checkField(t, 123.45, false, func() (interface{}, bool) { return yxdb.ReadFloat64WithName(`FixedDecimalField`) })
		checkField(t, `A`, false, func() (interface{}, bool) { return yxdb.ReadStringWithIndex(8) })
		checkField(t, `A`, false, func() (interface{}, bool) { return yxdb.ReadStringWithName(`StringField`) })
		checkField(t, `AB`, false, func() (interface{}, bool) { return yxdb.ReadStringWithIndex(9) })
		checkField(t, `AB`, false, func() (interface{}, bool) { return yxdb.ReadStringWithName(`WStringField`) })
		checkField(t, `ABC`, false, func() (interface{}, bool) { return yxdb.ReadStringWithIndex(10) })
		checkField(t, `ABC`, false, func() (interface{}, bool) { return yxdb.ReadStringWithName(`V_StringShortField`) })
		checkField(t, strings.Repeat(`B`, 500), false, func() (interface{}, bool) { return yxdb.ReadStringWithIndex(11) })
		checkField(t, strings.Repeat(`B`, 500), false, func() (interface{}, bool) { return yxdb.ReadStringWithName(`V_StringLongField`) })
		checkField(t, `XZY`, false, func() (interface{}, bool) { return yxdb.ReadStringWithIndex(12) })
		checkField(t, `XZY`, false, func() (interface{}, bool) { return yxdb.ReadStringWithName(`V_WStringShortField`) })
		checkField(t, strings.Repeat(`W`, 500), false, func() (interface{}, bool) { return yxdb.ReadStringWithIndex(13) })
		checkField(t, strings.Repeat(`W`, 500), false, func() (interface{}, bool) { return yxdb.ReadStringWithName(`V_WStringLongField`) })

		expected := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		checkField(t, expected, false, func() (interface{}, bool) { return yxdb.ReadTimeWithIndex(14) })
		checkField(t, expected, false, func() (interface{}, bool) { return yxdb.ReadTimeWithName(`DateField`) })

		expected = time.Date(2020, 2, 3, 4, 5, 6, 0, time.UTC)
		checkField(t, expected, false, func() (interface{}, bool) { return yxdb.ReadTimeWithIndex(15) })
		checkField(t, expected, false, func() (interface{}, bool) { return yxdb.ReadTimeWithName(`DateTimeField`) })

		read++
	}
	_ = yxdb.Close()
	if read != 1 {
		t.Fatalf(`expected 1 record read but got %v`, read)
	}
}

func TestLotsOfRecords(t *testing.T) {
	path := getPath(`LotsOfRecords.yxdb`)
	yxdb, err := yx.ReadFile(path)

	if err != nil {
		t.Fatalf(`expected no error but got %v`, err.Error())
	}

	sum := int64(0)
	for yxdb.Next() {
		value, isNull := yxdb.ReadInt64WithIndex(0)
		if isNull {
			t.Fatalf(`expected not null but got null`)
		}
		sum += value
	}
	if sum != 5000050000 {
		t.Fatalf(`expected 5000050000 but got %v`, sum)
	}
	_ = yxdb.Close()
}

func TestLoadReaderFromStream(t *testing.T) {
	path := getPath(`LotsOfRecords.yxdb`)
	file, _ := os.Open(path)
	yxdb, err := yx.ReadStream(file)

	if err != nil {
		t.Fatalf(`expected no error but got %v`, err.Error())
	}

	sum := int64(0)
	for yxdb.Next() {
		value, isNull := yxdb.ReadInt64WithIndex(0)
		if isNull {
			t.Fatalf(`expected not null but got null`)
		}
		sum += value
	}
	if sum != 5000050000 {
		t.Fatalf(`expected 5000050000 but got %v`, sum)
	}
	_ = yxdb.Close()
}

func TestTutorialData(t *testing.T) {
	path := getPath(`TutorialData.yxdb`)
	yxdb, err := yx.ReadFile(path)
	if err != nil {
		t.Fatalf(`expected no error but got %v`, err.Error())
	}
	mrCount := 0
	for yxdb.Next() {
		if value, _ := yxdb.ReadStringWithName(`Prefix`); value == `Mr` {
			mrCount++
		}
	}
	if mrCount != 4068 {
		t.Fatalf(`expected 4068 but got %v`, mrCount)
	}
}

func TestNewYxdb(t *testing.T) {
	path := getPath(`TestNewYxdb.yxdb`)
	yxdb, err := yx.ReadFile(path)

	if err != nil {
		t.Fatalf(`expected no error but got %v`, err.Error())
	}
	sum := byte(0)
	for yxdb.Next() {
		value, isNull := yxdb.ReadByteWithIndex(1)
		if isNull {
			t.Fatalf(`expected not null but got null`)
		}
		sum += value
	}
	if sum != 6 {
		t.Fatalf(`expected 6 but got %v`, sum)
	}
	_ = yxdb.Close()
}

func TestVeryLongField(t *testing.T) {
	path := getPath(`VeryLongField.yxdb`)
	yxdb, err := yx.ReadFile(path)
	if err != nil {
		t.Fatalf(`expected no error but got %v`, err.Error())
	}

	expectedSize := 604732

	yxdb.Next()
	blob := yxdb.ReadBlobWithIndex(1)
	if size := len(blob); size != expectedSize {
		t.Fatalf(`expected %v but got %v`, expectedSize, size)
	}

	yxdb.Next()
	blob = yxdb.ReadBlobWithIndex(1)
	if size := len(blob); size != 0 {
		t.Fatalf(`expected 0 but got %v`, size)
	}

	yxdb.Next()
	blob = yxdb.ReadBlobWithIndex(1)
	if size := len(blob); size != expectedSize {
		t.Fatalf(`expected %v but got %v`, expectedSize, size)
	}
}

func getPath(fileName string) string {
	return fmt.Sprintf(`test_files/%v`, fileName)
}

func checkField(t *testing.T, expected interface{}, isNull bool, getActual func() (interface{}, bool)) {
	actual, actualIsNull := getActual()
	if actual != expected || actualIsNull != isNull {
		t.Fatalf(`expected %v and isNull=%v but got %v and isNull=%v`, expected, isNull, actual, actualIsNull)
	}
}
