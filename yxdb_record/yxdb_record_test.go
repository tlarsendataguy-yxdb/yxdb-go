package yxdb_record_test

import (
	"fmt"
	"github.com/tlarsendataguy-yxdb/yxdb-go/meta_info_field"
	r "github.com/tlarsendataguy-yxdb/yxdb-go/yxdb_record"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestReadInt16Record(t *testing.T) {
	record := loadRecordWithValueColumn("Int16", 2)
	source := []byte{23, 0, 0}

	checkRecord(t, record, r.Int64, false, 3)
	checkIntValue(t, record, source, 23)
}

func TestReadInt32Record(t *testing.T) {
	record := loadRecordWithValueColumn("Int32", 4)
	source := []byte{23, 0, 0, 0, 0, 0}

	checkRecord(t, record, r.Int64, false, 5)
	checkIntValue(t, record, source, 23)
}

func TestReadInt64Record(t *testing.T) {
	record := loadRecordWithValueColumn("Int64", 8)
	source := []byte{23, 0, 0, 0, 0, 0, 0, 0, 0, 0}

	checkRecord(t, record, r.Int64, false, 9)
	checkIntValue(t, record, source, 23)
}

func TestReadFloatRecord(t *testing.T) {
	record := loadRecordWithValueColumn("Float", 4)
	source := []byte{205, 206, 140, 63, 0}

	checkRecord(t, record, r.Float64, false, 5)
	checkFloatValue(t, record, source, 1.1)
}

func TestReadDoubleRecord(t *testing.T) {
	record := loadRecordWithValueColumn("Double", 8)
	source := []byte{154, 155, 155, 155, 155, 155, 241, 63, 0}

	checkRecord(t, record, r.Float64, false, 9)
	checkFloatValue(t, record, source, 1.1)
}

func TestReadFixedDecimalRecord(t *testing.T) {
	record := loadRecordWithValueColumn("FixedDecimal", 10)
	source := []byte{49, 50, 51, 46, 52, 53, 0, 43, 67, 110, 0}

	checkRecord(t, record, r.Float64, false, 11)
	checkFloatValue(t, record, source, 123.45)
}

func TestReadStringRecord(t *testing.T) {
	record := loadRecordWithValueColumn("String", 15)
	source := []byte{104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 33, 0, 23, 77, 0}

	checkRecord(t, record, r.String, false, 16)
	checkStringValue(t, record, source, `hello world!`)
}

func TestReadWStringRecord(t *testing.T) {
	record := loadRecordWithValueColumn("WString", 15)
	source := []byte{104, 0, 101, 0, 108, 0, 108, 0, 111, 0, 32, 0, 119, 0, 111, 0, 114, 0, 108, 0, 100, 0, 33, 0, 0, 0, 23, 0, 77, 0, 0}

	checkRecord(t, record, r.String, false, 31)
	checkStringValue(t, record, source, `hello world!`)
}

func TestReadV_StringRecord(t *testing.T) {
	record := loadRecordWithValueColumn("V_String", 15)
	source := []byte{0, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8}

	checkRecord(t, record, r.String, true, 4)
	checkStringValue(t, record, source, ``)
}

func TestReadV_WStringRecord(t *testing.T) {
	record := loadRecordWithValueColumn("V_WString", 15)
	source := []byte{0, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8}

	checkRecord(t, record, r.String, true, 4)
	checkStringValue(t, record, source, ``)
}

func TestReadDateRecord(t *testing.T) {
	record := loadRecordWithValueColumn("Date", 10)
	source := []byte{50, 48, 50, 49, 45, 48, 49, 45, 48, 49, 0}

	checkRecord(t, record, r.Date, false, 11)
	checkTimeValue(t, record, source, time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC))
}

func TestReadDateTimeRecord(t *testing.T) {
	record := loadRecordWithValueColumn("DateTime", 19)
	source := []byte{50, 48, 50, 49, 45, 48, 49, 45, 48, 50, 32, 48, 51, 58, 48, 52, 58, 48, 53, 0}

	checkRecord(t, record, r.Date, false, 20)
	checkTimeValue(t, record, source, time.Date(2021, 1, 2, 3, 4, 5, 0, time.UTC))
}

func TestReadBoolRecord(t *testing.T) {
	record := loadRecordWithValueColumn("Bool", 1)
	source := []byte{1}

	checkRecord(t, record, r.Boolean, false, 1)
	checkBoolValue(t, record, source, true)
}

func TestReadByteRecord(t *testing.T) {
	record := loadRecordWithValueColumn("Byte", 1)
	source := []byte{23, 0}

	checkRecord(t, record, r.Byte, false, 2)
	checkByteValue(t, record, source, byte(23))
}

func TestReadBlobRecord(t *testing.T) {
	record := loadRecordWithValueColumn("Blob", 100)
	source := []byte{0, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8}

	checkRecord(t, record, r.Blob, true, 4)
	checkBlobValue(t, record, source, []byte{})
}

func TestReadSpatialObjRecord(t *testing.T) {
	record := loadRecordWithValueColumn("SpatialObj", 100)
	source := []byte{0, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8}

	checkRecord(t, record, r.Blob, true, 4)
	checkBlobValue(t, record, source, []byte{})
}

func checkRecord(t *testing.T, record *r.YxdbRecord, dataType r.DataType, hasVar bool, fixedSize int) {
	if fields := len(record.Fields); fields != 1 {
		t.Fatalf(`expected 1 field but got %v`, fields)
	}
	if name := record.Fields[0].Name; name != `value` {
		t.Fatalf(`expected 'value' field name but got '%v'`, name)
	}
	if actualType := record.Fields[0].Type; actualType != dataType {
		t.Fatalf(`expected '%v' data type but got %v`, dataType, actualType)
	}
	if hasVar != record.HasVar {
		t.Fatalf(`expected HasVar of %v but got %v`, hasVar, record.HasVar)
	}
	if fixedSize != record.FixedSize {
		t.Fatalf(`expected fixed size %v but got %v`, fixedSize, record.FixedSize)
	}
}

func checkIntValue(t *testing.T, record *r.YxdbRecord, source []byte, expected int64) {
	actual, isNull := record.ExtractInt64WithName(`value`, source)
	if isNull {
		t.Fatalf(`expected not null but got null`)
	}
	if actual != expected {
		t.Fatalf(`expected %v but got %v`, expected, actual)
	}
}

func checkFloatValue(t *testing.T, record *r.YxdbRecord, source []byte, expected float64) {
	actual, isNull := record.ExtractFloat64WithName(`value`, source)
	if isNull {
		t.Fatalf(`expected not null but got null`)
	}
	if math.Abs(actual-expected) > 0.001 {
		t.Fatalf(`expected %v but got %v`, expected, actual)
	}
}

func checkStringValue(t *testing.T, record *r.YxdbRecord, source []byte, expected string) {
	actual, isNull := record.ExtractStringWithName(`value`, source)
	if isNull {
		t.Fatalf(`expected not null but got null`)
	}
	if actual != expected {
		t.Fatalf(`expected %v but got %v`, expected, actual)
	}
}

func checkTimeValue(t *testing.T, record *r.YxdbRecord, source []byte, expected time.Time) {
	actual, isNull := record.ExtractTimeWithName(`value`, source)
	if isNull {
		t.Fatalf(`expected not null but got null`)
	}
	if actual != expected {
		t.Fatalf(`expected %v but got %v`, expected, actual)
	}
}

func checkBoolValue(t *testing.T, record *r.YxdbRecord, source []byte, expected bool) {
	actual, isNull := record.ExtractBoolWithName(`value`, source)
	if isNull {
		t.Fatalf(`expected not null but got null`)
	}
	if actual != expected {
		t.Fatalf(`expected %v but got %v`, expected, actual)
	}
}

func checkByteValue(t *testing.T, record *r.YxdbRecord, source []byte, expected byte) {
	actual, isNull := record.ExtractByteWithName(`value`, source)
	if isNull {
		t.Fatalf(`expected not null but got null`)
	}
	if actual != expected {
		t.Fatalf(`expected %v but got %v`, expected, actual)
	}
}

func checkBlobValue(t *testing.T, record *r.YxdbRecord, source []byte, expected []byte) {
	actual := record.ExtractBlobWithName(`value`, source)
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf(`expected %v but got %v`, expected, actual)
	}
}

func loadRecordWithValueColumn(dataType string, size int) *r.YxdbRecord {
	fields := []meta_info_field.MetaInfoField{
		{
			Name:  `value`,
			Type:  dataType,
			Size:  size,
			Scale: 0,
		},
	}
	record, err := r.FromFieldList(fields)
	if err != nil {
		panic(fmt.Sprintf("unexpected error: %v", err.Error()))
	}
	return record
}
