package extractors_test

import (
	"github.com/tlarsendataguy-yxdb/yxdb-go/extractors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestExtractInt16(t *testing.T) {
	extract := extractors.NewInt16Extractor(2)
	result, isNull := extract([]byte{0, 0, 10, 0, 0, 0})
	checkNotNull(t, result, isNull, int64(10))
}

func TestExtractNullInt16(t *testing.T) {
	extract := extractors.NewInt16Extractor(2)
	result, isNull := extract([]byte{0, 0, 10, 0, 1, 0})
	checkNull(t, result, isNull, int64(0))
}

func TestExtractInt32(t *testing.T) {
	extract := extractors.NewInt32Extractor(3)
	result, isNull := extract([]byte{0, 0, 0, 10, 0, 0, 0, 0})
	checkNotNull(t, result, isNull, int64(10))
}

func TestExtractNullInt32(t *testing.T) {
	extract := extractors.NewInt32Extractor(3)
	result, isNull := extract([]byte{0, 0, 0, 10, 0, 0, 0, 1})
	checkNull(t, result, isNull, int64(0))
}

func TestExtractInt64(t *testing.T) {
	extract := extractors.NewInt64Extractor(4)
	result, isNull := extract([]byte{0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 0})
	checkNotNull(t, result, isNull, int64(10))
}

func TestExtractNullInt64(t *testing.T) {
	extract := extractors.NewInt64Extractor(4)
	result, isNull := extract([]byte{0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 1})
	checkNull(t, result, isNull, int64(0))
}

func TestExtractBool(t *testing.T) {
	extract := extractors.NewBoolExtractor(4)
	result, isNull := extract([]byte{0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0})
	checkNotNull(t, result, isNull, true)
}

func TestExtractNullBool(t *testing.T) {
	extract := extractors.NewBoolExtractor(4)
	result, isNull := extract([]byte{0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1})
	checkNull(t, result, isNull, false)
}

func TestExtractByte(t *testing.T) {
	extract := extractors.NewByteExtractor(4)
	result, isNull := extract([]byte{0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 0})
	checkNotNull(t, result, isNull, byte(10))
}

func TestExtractNullByte(t *testing.T) {
	extract := extractors.NewByteExtractor(4)
	result, isNull := extract([]byte{0, 0, 0, 0, 10, 1, 0, 0, 0, 0, 0, 0, 1})
	checkNull(t, result, isNull, byte(0))
}

func TestExtractFloat(t *testing.T) {
	extract := extractors.NewFloatExtractor(4)
	result, isNull := extract([]byte{0, 0, 0, 0, 205, 206, 140, 63, 0, 0, 0, 0, 0})
	if isNull {
		t.Fatalf(`expected not null but got null`)
	}
	if math.Abs(result-1.1) > 0.0001 {
		t.Fatalf(`expected 1.1 but got %v`, result)
	}
}

func TestExtractNullFloat(t *testing.T) {
	extract := extractors.NewFloatExtractor(4)
	result, isNull := extract([]byte{0, 0, 0, 0, 205, 206, 140, 63, 1, 0, 0, 0, 1})
	checkNull(t, result, isNull, 0.0)
}

func TestExtractDouble(t *testing.T) {
	extract := extractors.NewDoubleExtractor(4)
	result, isNull := extract([]byte{0, 0, 0, 0, 154, 155, 155, 155, 155, 155, 241, 63, 0})
	if isNull {
		t.Fatalf(`expected not null but got null`)
	}
	if math.Abs(result-1.1) > 0.001 {
		t.Fatalf(`expected 1.1 but got %v`, result)
	}
}

func TestExtractNullDouble(t *testing.T) {
	extract := extractors.NewDoubleExtractor(4)
	result, isNull := extract([]byte{0, 0, 0, 0, 154, 155, 155, 155, 155, 155, 241, 63, 1})
	checkNull(t, result, isNull, 0.0)
}

func TestExtractDate(t *testing.T) {
	extract := extractors.NewDateExtractor(4)
	result, isNull := extract([]byte{0, 0, 0, 0, 50, 48, 50, 49, 45, 48, 49, 45, 48, 49, 0})
	checkNotNull(t, result, isNull, time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC))
}

func TestExtractNullDate(t *testing.T) {
	extract := extractors.NewDateExtractor(4)
	result, isNull := extract([]byte{0, 0, 0, 0, 50, 48, 50, 49, 45, 48, 49, 45, 48, 49, 1})
	checkNull(t, result, isNull, time.Time{})
}

func TestExtractDateTime(t *testing.T) {
	extract := extractors.NewDateTimeExtractor(4)
	result, isNull := extract([]byte{0, 0, 0, 0, 50, 48, 50, 49, 45, 48, 49, 45, 48, 50, 32, 48, 51, 58, 48, 52, 58, 48, 53, 0})
	checkNotNull(t, result, isNull, time.Date(2021, 1, 2, 3, 4, 5, 0, time.UTC))
}

func TestExtractNullDateTime(t *testing.T) {
	extract := extractors.NewDateTimeExtractor(4)
	result, isNull := extract([]byte{0, 0, 0, 0, 50, 48, 50, 49, 45, 48, 49, 45, 48, 50, 32, 48, 51, 58, 48, 52, 58, 48, 53, 1})
	checkNull(t, result, isNull, time.Time{})
}

func TestExtractString(t *testing.T) {
	extract := extractors.NewStringExtractor(2, 15)
	result, isNull := extract([]byte{0, 0, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 33, 0, 23, 77, 0})
	checkNotNull(t, result, isNull, `hello world!`)
}

func TestExtractFullString(t *testing.T) {
	extract := extractors.NewStringExtractor(2, 5)
	result, isNull := extract([]byte{0, 0, 104, 101, 108, 108, 111, 0})
	checkNotNull(t, result, isNull, `hello`)
}

func TestExtractNullString(t *testing.T) {
	extract := extractors.NewStringExtractor(2, 5)
	result, isNull := extract([]byte{0, 0, 104, 101, 108, 108, 111, 1})
	checkNull(t, result, isNull, ``)
}

func TestExtractEmptyString(t *testing.T) {
	extract := extractors.NewStringExtractor(2, 5)
	result, isNull := extract([]byte{0, 0, 0, 101, 108, 108, 111, 0})
	checkNotNull(t, result, isNull, ``)
}

func TestExtractFixedDecimal(t *testing.T) {
	extract := extractors.NewFixedDecimalExtractor(2, 10)
	result, isNull := extract([]byte{0, 0, 49, 50, 51, 46, 52, 53, 0, 43, 67, 110, 0})
	checkNotNull(t, result, isNull, 123.45)
}

func TestExtractNullFixedDecimal(t *testing.T) {
	extract := extractors.NewFixedDecimalExtractor(2, 10)
	result, isNull := extract([]byte{0, 0, 49, 50, 51, 46, 52, 53, 0, 43, 67, 110, 1})
	checkNull(t, result, isNull, 0.0)
}

func TestExtractWString(t *testing.T) {
	extract := extractors.NewWStringExtractor(2, 15)
	result, isNull := extract([]byte{0, 0, 104, 0, 101, 0, 108, 0, 108, 0, 111, 0, 32, 0, 119, 0, 111, 0, 114, 0, 108, 0, 100, 0, 0, 0, 12, 0, 44, 0, 55, 0, 0})
	checkNotNull(t, result, isNull, `hello world`)
}

func TestExtractNullWString(t *testing.T) {
	extract := extractors.NewWStringExtractor(2, 15)
	result, isNull := extract([]byte{0, 0, 104, 0, 101, 0, 108, 0, 108, 0, 111, 0, 32, 0, 119, 0, 111, 0, 114, 0, 108, 0, 100, 0, 0, 0, 12, 0, 44, 0, 55, 0, 1})
	checkNull(t, result, isNull, ``)
}

func TestExtractEmptyWString(t *testing.T) {
	extract := extractors.NewWStringExtractor(2, 15)
	result, isNull := extract([]byte{0, 0, 0, 0, 101, 0, 108, 0, 108, 0, 111, 0, 32, 0, 119, 0, 111, 0, 114, 0, 108, 0, 100, 0, 0, 0, 12, 0, 44, 0, 55, 0, 0})
	checkNotNull(t, result, isNull, ``)
}

func TestExtractNormalBlob(t *testing.T) {
	extract := extractors.NewBlobExtractor(6)
	result := extract(normalBlob)
	expected := []byte(strings.Repeat("B", 200))
	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("expected\n%v\nbut got\n%v", expected, result)
	}
}

func TestExtractSmallBlob(t *testing.T) {
	extract := extractors.NewBlobExtractor(6)
	result := extract(smallBlob)
	expected := []byte(strings.Repeat("B", 100))
	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("expected\n%v\nbut got\n%v", expected, result)
	}
}

func TestExtractTinyBlob(t *testing.T) {
	extract := extractors.NewBlobExtractor(6)
	result := extract([]byte{1, 0, 65, 0, 0, 32, 66, 0, 0, 16, 0, 0, 0, 0})
	expected := []byte{66}
	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("expected\n%v\nbut got\n%v", expected, result)
	}
}

func TestExtractEmptyBlob(t *testing.T) {
	extract := extractors.NewBlobExtractor(6)
	result := extract([]byte{1, 0, 65, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0})
	expected := make([]byte, 0)
	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("expected\n%v\nbut got\n%v", expected, result)
	}
}

func TestExtractNullBlob(t *testing.T) {
	extract := extractors.NewBlobExtractor(6)
	result := extract([]byte{1, 0, 65, 0, 0, 32, 1, 0, 0, 0, 0, 0, 0, 0})
	if result != nil {
		t.Fatalf(`expected nil but got %v`, result)
	}
}

func TestExtractV_String(t *testing.T) {
	extract := extractors.NewV_StringExtractor(6)
	result, isNull := extract(smallBlob)
	checkNotNull(t, result, isNull, strings.Repeat(`B`, 100))
}

func TestExtractNullV_String(t *testing.T) {
	extract := extractors.NewV_StringExtractor(2)
	result, isNull := extract([]byte{0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8})
	checkNull(t, result, isNull, ``)
}

func TestExtractEmptyV_String(t *testing.T) {
	extract := extractors.NewV_StringExtractor(2)
	result, isNull := extract([]byte{0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8})
	checkNotNull(t, result, isNull, ``)
}

func TestExtractV_WString(t *testing.T) {
	extract := extractors.NewV_WStringExtractor(2)
	result, isNull := extract(normalBlob)
	checkNotNull(t, result, isNull, strings.Repeat(`A`, 100))
}

func TestExtractNullV_WString(t *testing.T) {
	extract := extractors.NewV_WStringExtractor(2)
	result, isNull := extract([]byte{0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8})
	checkNull(t, result, isNull, ``)
}

func TestExtractEmptyV_WString(t *testing.T) {
	extract := extractors.NewV_WStringExtractor(2)
	result, isNull := extract([]byte{0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8})
	checkNotNull(t, result, isNull, ``)
}

func checkNull(t *testing.T, value interface{}, isNull bool, expectedDefault interface{}) {
	if !isNull {
		t.Fatalf(`expected null but it was not`)
	}
	if value != expectedDefault {
		t.Fatalf(`expected %v but got %v`, expectedDefault, value)
	}
}

func checkNotNull(t *testing.T, value interface{}, isNull bool, expected interface{}) {
	if isNull {
		t.Fatalf(`expected not null but it was`)
	}
	if value != expected {
		t.Fatalf(`expected %v but got %v`, expected, value)
	}
}

var normalBlob = []byte{
	1, 0, 12, 0, 0, 0, 212, 0, 0, 0, 152, 1, 0, 0, 144, 1, 0, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65,
	0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0,
	65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65,
	0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0,
	65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65,
	0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0,
	65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65,
	0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0,
	65, 0, 144, 1, 0, 0, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
	66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
	66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
	66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
	66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
	66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
	66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
	66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
	66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
}

var smallBlob = []byte{
	1, 0, 12, 0, 0, 0, 109, 0, 0, 0, 202, 0, 0, 0, 201, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0,
	65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65,
	0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0,
	65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 201, 66, 66, 66, 66, 66, 66,
	66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
	66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
	66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
	66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
}
