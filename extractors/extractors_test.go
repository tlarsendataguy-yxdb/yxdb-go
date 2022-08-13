package extractors_test

import (
	"github.com/tlarsendataguy-yxdb/yxdb-go/extractors"
	"testing"
)

func TestExtractInt16(t *testing.T) {
	extract := extractors.NewInt16Extractor(2)
	result, isNull := extract([]byte{0, 0, 10, 0, 0, 0})
	if isNull {
		t.Fatalf(`expected not null but it was`)
	}
	if result != 10 {
		t.Fatalf(`expected 10 but got %v`, result)
	}
}

func TestExtractNullInt16(t *testing.T) {
	extract := extractors.NewInt16Extractor(2)
	result, isNull := extract([]byte{0, 0, 10, 0, 1, 0})
	if !isNull {
		t.Fatalf(`expected null but it was not`)
	}
	if result != 0 {
		t.Fatalf(`expected 0 but got %v`, result)
	}
}
