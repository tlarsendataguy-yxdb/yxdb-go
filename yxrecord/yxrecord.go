package yxrecord

import (
	"errors"
	"fmt"
	e "github.com/tlarsendataguy-yxdb/yxdb-go/extractors"
	m "github.com/tlarsendataguy-yxdb/yxdb-go/metafield"
	"time"
)

type DataType int

const (
	Blob DataType = iota
	Boolean
	Byte
	Date
	Float64
	Int64
	String
)

// YxdbField contains the name and type of field in a .yxdb file.
type YxdbField struct {
	Name string
	Type DataType
}

type YxdbRecord struct {
	Fields            []YxdbField
	FixedSize         int
	HasVar            bool
	nameToIndex       map[string]int
	boolExtractors    map[int]e.BoolExtractor
	byteExtractors    map[int]e.ByteExtractor
	int64Extractors   map[int]e.Int64Extractor
	float64Extractors map[int]e.Float64Extractor
	stringExtractors  map[int]e.StringExtractor
	timeExtractors    map[int]e.TimeExtractor
	blobExtractors    map[int]e.BlobExtractor
}

func FromFieldList(fields []m.MetaInfoField) (*YxdbRecord, error) {
	record := &YxdbRecord{
		Fields:            make([]YxdbField, 0, len(fields)),
		nameToIndex:       make(map[string]int, len(fields)),
		boolExtractors:    make(map[int]e.BoolExtractor),
		byteExtractors:    make(map[int]e.ByteExtractor),
		int64Extractors:   make(map[int]e.Int64Extractor),
		float64Extractors: make(map[int]e.Float64Extractor),
		stringExtractors:  make(map[int]e.StringExtractor),
		timeExtractors:    make(map[int]e.TimeExtractor),
		blobExtractors:    make(map[int]e.BlobExtractor),
	}
	startAt := 0
	for _, field := range fields {
		switch field.Type {
		case `Int16`:
			record.addInt64Extractor(field.Name, e.NewInt16Extractor(startAt))
			startAt += 3
		case `Int32`:
			record.addInt64Extractor(field.Name, e.NewInt32Extractor(startAt))
			startAt += 5
		case `Int64`:
			record.addInt64Extractor(field.Name, e.NewInt64Extractor(startAt))
			startAt += 9
		case `Float`:
			record.addFloat64Extractor(field.Name, e.NewFloatExtractor(startAt))
			startAt += 5
		case `Double`:
			record.addFloat64Extractor(field.Name, e.NewDoubleExtractor(startAt))
			startAt += 9
		case `FixedDecimal`:
			record.addFloat64Extractor(field.Name, e.NewFixedDecimalExtractor(startAt, field.Size))
			startAt += field.Size + 1
		case `String`:
			record.addStringExtractor(field.Name, e.NewStringExtractor(startAt, field.Size))
			startAt += field.Size + 1
		case `WString`:
			record.addStringExtractor(field.Name, e.NewWStringExtractor(startAt, field.Size))
			startAt += (field.Size * 2) + 1
		case `V_String`:
			record.addStringExtractor(field.Name, e.NewV_StringExtractor(startAt))
			startAt += 4
			record.HasVar = true
		case `V_WString`:
			record.addStringExtractor(field.Name, e.NewV_WStringExtractor(startAt))
			startAt += 4
			record.HasVar = true
		case `Date`:
			record.addTimeExtractor(field.Name, e.NewDateExtractor(startAt))
			startAt += 11
		case `DateTime`:
			record.addTimeExtractor(field.Name, e.NewDateTimeExtractor(startAt))
			startAt += 20
		case `Bool`:
			record.addBoolExtractor(field.Name, e.NewBoolExtractor(startAt))
			startAt += 1
		case `Byte`:
			record.addByteExtractor(field.Name, e.NewByteExtractor(startAt))
			startAt += 2
		case `Blob`, `SpatialObj`:
			record.addBlobExtractor(field.Name, e.NewBlobExtractor(startAt))
			startAt += 4
			record.HasVar = true
		default:
			return nil, errors.New("field type not supported, file is not a valid yxdb")
		}
	}
	record.FixedSize = startAt
	return record, nil
}

func (y *YxdbRecord) ExtractInt64WithIndex(index int, buffer []byte) (int64, bool) {
	extractor, ok := y.int64Extractors[index]
	if !ok {
		panic(invalidIndex(index, `int64`))
	}
	return extractor(buffer)
}

func (y *YxdbRecord) ExtractInt64WithName(name string, buffer []byte) (int64, bool) {
	index, ok := y.nameToIndex[name]
	if !ok {
		panic(invalidName(name))
	}
	return y.ExtractInt64WithIndex(index, buffer)
}

func (y *YxdbRecord) ExtractFloat64WithIndex(index int, buffer []byte) (float64, bool) {
	extractor, ok := y.float64Extractors[index]
	if !ok {
		panic(invalidIndex(index, `float64`))
	}
	return extractor(buffer)
}

func (y *YxdbRecord) ExtractFloat64WithName(name string, buffer []byte) (float64, bool) {
	index, ok := y.nameToIndex[name]
	if !ok {
		panic(invalidName(name))
	}
	return y.ExtractFloat64WithIndex(index, buffer)
}

func (y *YxdbRecord) ExtractStringWithIndex(index int, buffer []byte) (string, bool) {
	extractor, ok := y.stringExtractors[index]
	if !ok {
		panic(invalidIndex(index, `string`))
	}
	return extractor(buffer)
}

func (y *YxdbRecord) ExtractStringWithName(name string, buffer []byte) (string, bool) {
	index, ok := y.nameToIndex[name]
	if !ok {
		panic(invalidName(name))
	}
	return y.ExtractStringWithIndex(index, buffer)
}

func (y *YxdbRecord) ExtractTimeWithIndex(index int, buffer []byte) (time.Time, bool) {
	extractor, ok := y.timeExtractors[index]
	if !ok {
		panic(invalidIndex(index, `time`))
	}
	return extractor(buffer)
}

func (y *YxdbRecord) ExtractTimeWithName(name string, buffer []byte) (time.Time, bool) {
	index, ok := y.nameToIndex[name]
	if !ok {
		panic(invalidName(name))
	}
	return y.ExtractTimeWithIndex(index, buffer)
}

func (y *YxdbRecord) ExtractBoolWithIndex(index int, buffer []byte) (bool, bool) {
	extractor, ok := y.boolExtractors[index]
	if !ok {
		panic(invalidIndex(index, `bool`))
	}
	return extractor(buffer)
}

func (y *YxdbRecord) ExtractBoolWithName(name string, buffer []byte) (bool, bool) {
	index, ok := y.nameToIndex[name]
	if !ok {
		panic(invalidName(name))
	}
	return y.ExtractBoolWithIndex(index, buffer)
}

func (y *YxdbRecord) ExtractByteWithIndex(index int, buffer []byte) (byte, bool) {
	extractor, ok := y.byteExtractors[index]
	if !ok {
		panic(invalidIndex(index, `byte`))
	}
	return extractor(buffer)
}

func (y *YxdbRecord) ExtractByteWithName(name string, buffer []byte) (byte, bool) {
	index, ok := y.nameToIndex[name]
	if !ok {
		panic(invalidName(name))
	}
	return y.ExtractByteWithIndex(index, buffer)
}

func (y *YxdbRecord) ExtractBlobWithIndex(index int, buffer []byte) []byte {
	extractor, ok := y.blobExtractors[index]
	if !ok {
		panic(invalidIndex(index, `blob`))
	}
	return extractor(buffer)
}

func (y *YxdbRecord) ExtractBlobWithName(name string, buffer []byte) []byte {
	index, ok := y.nameToIndex[name]
	if !ok {
		panic(invalidName(name))
	}
	return y.ExtractBlobWithIndex(index, buffer)
}

func (y *YxdbRecord) addInt64Extractor(name string, extractor e.Int64Extractor) {
	index := y.addFieldNameToIndexMap(name, Int64)
	y.int64Extractors[index] = extractor
}

func (y *YxdbRecord) addFloat64Extractor(name string, extractor e.Float64Extractor) {
	index := y.addFieldNameToIndexMap(name, Float64)
	y.float64Extractors[index] = extractor
}

func (y *YxdbRecord) addStringExtractor(name string, extractor e.StringExtractor) {
	index := y.addFieldNameToIndexMap(name, String)
	y.stringExtractors[index] = extractor
}

func (y *YxdbRecord) addTimeExtractor(name string, extractor e.TimeExtractor) {
	index := y.addFieldNameToIndexMap(name, Date)
	y.timeExtractors[index] = extractor
}

func (y *YxdbRecord) addBoolExtractor(name string, extractor e.BoolExtractor) {
	index := y.addFieldNameToIndexMap(name, Boolean)
	y.boolExtractors[index] = extractor
}

func (y *YxdbRecord) addByteExtractor(name string, extractor e.ByteExtractor) {
	index := y.addFieldNameToIndexMap(name, Byte)
	y.byteExtractors[index] = extractor
}

func (y *YxdbRecord) addBlobExtractor(name string, extractor e.BlobExtractor) {
	index := y.addFieldNameToIndexMap(name, Blob)
	y.blobExtractors[index] = extractor
}

func (y *YxdbRecord) addFieldNameToIndexMap(name string, dataType DataType) int {
	index := len(y.Fields)
	y.Fields = append(y.Fields, YxdbField{
		Name: name,
		Type: dataType,
	})
	y.nameToIndex[name] = index
	return index
}

func invalidIndex(index int, dataType string) string {
	return fmt.Sprintf(`field at index %v is not a %v field`, index, dataType)
}

func invalidName(name string) string {
	return fmt.Sprintf(`field '%v' does not exist`, name)
}
