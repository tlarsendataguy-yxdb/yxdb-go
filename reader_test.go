package yxdb_test

import (
	"fmt"
	yx "github.com/tlarsendataguy-yxdb/yxdb-go"
	"os"
	"strings"
	"testing"
	"time"
)

func TestGetReader(t *testing.T) {
	yxdb := getYxdb(t, `AllNormalFields.yxdb`)

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
	yxdb := getYxdb(t, `LotsOfRecords.yxdb`)

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
	yxdb := getYxdb(t, `TutorialData.yxdb`)

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
	yxdb := getYxdb(t, `TestNewYxdb.yxdb`)

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
	yxdb := getYxdb(t, `VeryLongField.yxdb`)

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

func TestReadStringFromNonStringIndex(t *testing.T) {
	defer checkPanic(t, `field at index 0 is not a string field`)()
	yxdb := getYxdb(t, `AllNormalFields.yxdb`)

	yxdb.ReadStringWithIndex(0)
}

func TestReadBoolFromNonBoolIndex(t *testing.T) {
	defer checkPanic(t, `field at index 0 is not a bool field`)()
	yxdb := getYxdb(t, `AllNormalFields.yxdb`)

	yxdb.ReadBoolWithIndex(0)
}

func TestReadBlobFromNonBlobIndex(t *testing.T) {
	defer checkPanic(t, `field at index 0 is not a blob field`)()
	yxdb := getYxdb(t, `AllNormalFields.yxdb`)

	yxdb.ReadBlobWithIndex(0)
}

func TestReadTimeFromNonTimeIndex(t *testing.T) {
	defer checkPanic(t, `field at index 0 is not a time field`)()
	yxdb := getYxdb(t, `AllNormalFields.yxdb`)

	yxdb.ReadTimeWithIndex(0)
}

func TestReadFloat64FromNonFloat64Index(t *testing.T) {
	defer checkPanic(t, `field at index 0 is not a float64 field`)()
	yxdb := getYxdb(t, `AllNormalFields.yxdb`)

	yxdb.ReadFloat64WithIndex(0)
}

func TestReadInt64FromNonInt64Index(t *testing.T) {
	defer checkPanic(t, `field at index 0 is not a int64 field`)()
	yxdb := getYxdb(t, `AllNormalFields.yxdb`)

	yxdb.ReadInt64WithIndex(0)
}

func TestReadByteFromNonByteIndex(t *testing.T) {
	defer checkPanic(t, `field at index 1 is not a byte field`)()
	yxdb := getYxdb(t, `AllNormalFields.yxdb`)

	yxdb.ReadByteWithIndex(1)
}

func TestReadInvalidStringField(t *testing.T) {
	defer checkPanic(t, `field 'invalid' does not exist`)()
	yxdb := getYxdb(t, `AllNormalFields.yxdb`)

	yxdb.ReadStringWithName(`invalid`)
}

func TestReadInvalidBoolField(t *testing.T) {
	defer checkPanic(t, `field 'invalid' does not exist`)()
	yxdb := getYxdb(t, `AllNormalFields.yxdb`)

	yxdb.ReadBoolWithName(`invalid`)
}

func TestReadInvalidBlobField(t *testing.T) {
	defer checkPanic(t, `field 'invalid' does not exist`)()
	yxdb := getYxdb(t, `AllNormalFields.yxdb`)

	yxdb.ReadBlobWithName(`invalid`)
}

func TestReadInvalidTimeField(t *testing.T) {
	defer checkPanic(t, `field 'invalid' does not exist`)()
	yxdb := getYxdb(t, `AllNormalFields.yxdb`)

	yxdb.ReadTimeWithName(`invalid`)
}

func TestReadInvalidFloat64Field(t *testing.T) {
	defer checkPanic(t, `field 'invalid' does not exist`)()
	yxdb := getYxdb(t, `AllNormalFields.yxdb`)

	yxdb.ReadFloat64WithName(`invalid`)
}

func TestReadInvalidInt64Field(t *testing.T) {
	defer checkPanic(t, `field 'invalid' does not exist`)()
	yxdb := getYxdb(t, `AllNormalFields.yxdb`)

	yxdb.ReadInt64WithName(`invalid`)
}

func TestReadInvalidByteField(t *testing.T) {
	defer checkPanic(t, `field 'invalid' does not exist`)()
	yxdb := getYxdb(t, `AllNormalFields.yxdb`)

	yxdb.ReadByteWithName(`invalid`)
}

func TestInvalidFile(t *testing.T) {
	_, err := yx.ReadFile(getPath(`invalid.txt`))
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	if err.Error() != `file is not a valid YXDB format` {
		t.Fatalf(`expected 'file is not a valid YXDB format' but got '%v'`, err.Error())
	}
}

func TestInvalidSmallFile(t *testing.T) {
	_, err := yx.ReadFile(getPath(`invalidSmall.txt`))
	if err == nil {
		t.Fatalf(`expected an error but got none`)
	}
	if err.Error() != `file is not a valid YXDB format` {
		t.Fatalf(`expected 'file is not a valid YXDB format' but got '%v'`, err.Error())
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

func getYxdb(t *testing.T, fileName string) yx.Reader {
	yxdb, err := yx.ReadFile(getPath(fileName))
	if err != nil {
		t.Fatalf(`expected no error but got: %v`, err.Error())
	}
	return yxdb
}

func checkPanic(t *testing.T, expectedMsg string) func() {
	return func() {
		r := recover()
		if r == nil {
			t.Fatalf(`expected a panic but it did not occur`)
		}
		if r != expectedMsg {
			t.Fatalf(`expected '%v' but got '%v'`, expectedMsg, r)
		}
	}
}
