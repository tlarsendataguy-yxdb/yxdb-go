package lzf_test

import (
	l "github.com/tlarsendataguy-yxdb/yxdb-go/lzf"
	"reflect"
	"testing"
)

func TestEmptyInput(t *testing.T) {
	performTest([]byte{}, []byte{}, t)
}

func TestOutputArrayIsTooSmall(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	inData := []byte{0, 25}
	outData := make([]byte, 0)
	lzf := l.Lzf{InBuffer: inData, OutBuffer: outData}

	lzf.Decompress(2)
}

func TestSmallControlValuesDoSimpleCopies(t *testing.T) {
	performTest([]byte{4, 1, 2, 3, 4, 5}, []byte{1, 2, 3, 4, 5}, t)
}

func TestMultipleSmallControlValues(t *testing.T) {
	performTest([]byte{2, 1, 2, 3, 1, 1, 2}, []byte{1, 2, 3, 1, 2}, t)
}

func TestExpandLargeControlValues(t *testing.T) {
	performTest([]byte{2, 1, 2, 3, 32, 1}, []byte{1, 2, 3, 2, 3, 2}, t)
}

func TestLargeControlValuesWithLengthOf7(t *testing.T) {
	performTest(
		[]byte{8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 224, 1, 8},
		[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1},
		t,
	)
}

func TestOutputArrayTooSmallForLargeControlValues(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	inData := []byte{8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 224, 1, 8}
	outData := make([]byte, 17)
	lzf := l.Lzf{InBuffer: inData, OutBuffer: outData}

	lzf.Decompress(13)
}

func TestResetLzfAndStartAgain(t *testing.T) {
	inData := []byte{4, 1, 2, 3, 4, 5}
	outData := make([]byte, 5)
	lzf := l.Lzf{InBuffer: inData, OutBuffer: outData}

	lzf.Decompress(6)

	inData[0] = 2
	inData[1] = 6
	inData[2] = 7
	inData[3] = 8

	written := lzf.Decompress(4)
	if written != 3 {
		t.Fatalf(`expected written 3 but got %v`, written)
	}
	expected := []byte{6, 7, 8, 4, 5}
	if !reflect.DeepEqual(expected, outData) {
		t.Fatalf(`expected %v but got %v`, expected, outData)
	}
}

func performTest(inData []byte, expected []byte, t *testing.T) {
	outSize := len(expected)
	outData := make([]byte, outSize)
	lzf := l.Lzf{InBuffer: inData, OutBuffer: outData}

	written := lzf.Decompress(len(inData))
	if written != outSize {
		t.Fatalf(`expected %v written but got %v`, outSize, written)
	}
	if !reflect.DeepEqual(expected, outData) {
		t.Fatalf(`expected %v but got %v`, expected, outData)
	}
}
