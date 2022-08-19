// Package yxdb reads and parses .yxdb data files.
package yxdb

import (
	"encoding/binary"
	"encoding/xml"
	"errors"
	"github.com/tlarsendataguy-yxdb/yxdb/bufrecord"
	"github.com/tlarsendataguy-yxdb/yxdb/metafield"
	"github.com/tlarsendataguy-yxdb/yxdb/yxrecord"
	"io"
	"os"
	"reflect"
	"time"
	"unicode/utf16"
	"unsafe"
)

type metaInfo struct {
	Fields           []metafield.MetaInfoField `xml:"Field"`
	RecordInfoFields []metafield.MetaInfoField `xml:"RecordInfo>Field"`
}

// A Reader is the interface that reads and parses .yxdb files.
//
// Instantiate a Reader using the ReadFile and ReadStream functions.
type Reader interface {
	io.Closer

	// ListFields returns the list of fields contained in the .yxdb file and their data type.
	ListFields() []yxrecord.YxdbField

	// Next iterates through the records in a .yxdb file, returning true if there are more records and false if
	// all records have been read.
	Next() bool

	// NumRecords returns the number of records in the .yxdb file.
	NumRecords() int64

	// MetaInfoStr returns the XML metadata, as a string, of the fields contained in the .yxdb file.
	MetaInfoStr() string

	// ReadByteWithIndex reads a byte field at the specified field index.
	//
	// If the field at the specified index is not a byte field, ReadByteWithIndex will panic.
	ReadByteWithIndex(int) (byte, bool)

	// ReadByteWithName reads a byte field with the specified name.
	//
	// If the name is not valid or the field with the specified name is not a byte field, ReadByteWithName will panic.
	ReadByteWithName(string) (byte, bool)

	// ReadBoolWithIndex reads a boolean field at the specified field index.
	//
	// If the field at the specified index is not a boolean field, ReadBoolWithIndex will panic.
	ReadBoolWithIndex(int) (bool, bool)

	// ReadBoolWithName reads a boolean field with the specified name.
	//
	// If the name is not valid or the field with the specified name is not a boolean field, ReadBoolWithName will panic.
	ReadBoolWithName(string) (bool, bool)

	// ReadInt64WithIndex reads an integer field at the specified field index.
	//
	// If the field at the specified index is not an integer field, ReadInt64WithIndex will panic.
	ReadInt64WithIndex(int) (int64, bool)

	// ReadInt64WithName reads an integer field with the specified name.
	//
	// If the name is not valid or the field with the specified name is not an integer field, ReadInt64WithName will panic.
	ReadInt64WithName(string) (int64, bool)

	// ReadFloat64WithIndex reads a numeric field at the specified field index.
	//
	// If the field at the specified index is not a numeric field, ReadFloat64WithIndex will panic.
	ReadFloat64WithIndex(int) (float64, bool)

	// ReadFloat64WithName reads a numeric field with the specified name.
	//
	// If the name is not valid or the field with the specified name is not a numeric field, ReadFloat64WithName will panic.
	ReadFloat64WithName(string) (float64, bool)

	// ReadStringWithIndex reads a string field at the specified field index.
	//
	// If the field at the specified index is not a string field, ReadStringWithIndex will panic.
	ReadStringWithIndex(int) (string, bool)

	// ReadStringWithName reads a string field with the specified name.
	//
	// If the name is not valid or the field with the specified name is not a string field, ReadStringWithName will panic.
	ReadStringWithName(string) (string, bool)

	// ReadTimeWithIndex reads a date/datetime field at the specified field index.
	//
	// If the field at the specified index is not a date/datetime field, ReadTimeWithIndex will panic.
	ReadTimeWithIndex(int) (time.Time, bool)

	// ReadTimeWithName reads a date/datetime field with the specified name.
	//
	// If the name is not valid or the field with the specified name is not a date/datetime field, ReadTimeWithName will panic.
	ReadTimeWithName(string) (time.Time, bool)

	// ReadBlobWithIndex reads a binary field at the specified field index.
	//
	// If the field at the specified index is not a binary field, ReadBlobWithIndex will panic.
	ReadBlobWithIndex(int) []byte

	// ReadBlobWithName reads a binary field with the specified name.
	//
	// If the name is not valid or the field with the specified name is not a binary field, ReadBlobWithName will panic.
	ReadBlobWithName(string) []byte
}

// ReadFile instantiates a Reader from the specified file path.
//
// If the file does not exist, cannot be opened, or is not a valid .yxdb file, ReadFile will return an error.
func ReadFile(path string) (Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	reader := &r{
		stream: file,
	}
	err = reader.loadHeaderAndMetaInfo()
	if err != nil {
		reader.close()
		return nil, errors.New(`the file is not a valid yxdb format`)
	}

	return reader, nil
}

// ReadStream instantiates a Reader from the specified io.ReadCloser.
//
// If the stream encounters an error or is not a valid .yxdb files, ReadStream will return an error.
func ReadStream(stream io.ReadCloser) (Reader, error) {
	reader := &r{
		stream: stream,
	}
	err := reader.loadHeaderAndMetaInfo()
	if err != nil {
		reader.close()
		return nil, err
	}
	return reader, nil
}

type r struct {
	stream       io.ReadCloser
	fields       []metafield.MetaInfoField
	metaInfoSize int
	numRecords   int64
	record       *yxrecord.YxdbRecord
	recordReader *bufrecord.BufferedRecordReader
	metaInfoStr  string
}

func (r *r) ListFields() []yxrecord.YxdbField {
	return r.record.Fields
}

func (r *r) Close() error {
	return r.stream.Close()
}

func (r *r) Next() bool {
	return r.recordReader.NextRecord()
}

func (r *r) NumRecords() int64 {
	return r.numRecords
}

func (r *r) MetaInfoStr() string {
	return r.metaInfoStr
}

func (r *r) ReadByteWithIndex(index int) (byte, bool) {
	return r.record.ExtractByteWithIndex(index, r.recordReader.RecordBuffer)
}

func (r *r) ReadByteWithName(name string) (byte, bool) {
	return r.record.ExtractByteWithName(name, r.recordReader.RecordBuffer)
}

func (r *r) ReadBoolWithIndex(index int) (bool, bool) {
	return r.record.ExtractBoolWithIndex(index, r.recordReader.RecordBuffer)
}

func (r *r) ReadBoolWithName(name string) (bool, bool) {
	return r.record.ExtractBoolWithName(name, r.recordReader.RecordBuffer)
}

func (r *r) ReadInt64WithIndex(index int) (int64, bool) {
	return r.record.ExtractInt64WithIndex(index, r.recordReader.RecordBuffer)
}

func (r *r) ReadInt64WithName(name string) (int64, bool) {
	return r.record.ExtractInt64WithName(name, r.recordReader.RecordBuffer)
}

func (r *r) ReadFloat64WithIndex(index int) (float64, bool) {
	return r.record.ExtractFloat64WithIndex(index, r.recordReader.RecordBuffer)
}

func (r *r) ReadFloat64WithName(name string) (float64, bool) {
	return r.record.ExtractFloat64WithName(name, r.recordReader.RecordBuffer)
}

func (r *r) ReadStringWithIndex(index int) (string, bool) {
	return r.record.ExtractStringWithIndex(index, r.recordReader.RecordBuffer)
}

func (r *r) ReadStringWithName(name string) (string, bool) {
	return r.record.ExtractStringWithName(name, r.recordReader.RecordBuffer)
}

func (r *r) ReadTimeWithIndex(index int) (time.Time, bool) {
	return r.record.ExtractTimeWithIndex(index, r.recordReader.RecordBuffer)
}

func (r *r) ReadTimeWithName(name string) (time.Time, bool) {
	return r.record.ExtractTimeWithName(name, r.recordReader.RecordBuffer)
}

func (r *r) ReadBlobWithIndex(index int) []byte {
	return r.record.ExtractBlobWithIndex(index, r.recordReader.RecordBuffer)
}

func (r *r) ReadBlobWithName(name string) []byte {
	return r.record.ExtractBlobWithName(name, r.recordReader.RecordBuffer)
}

func (r *r) loadHeaderAndMetaInfo() error {
	r.fields = make([]metafield.MetaInfoField, 0)
	header, err := r.getHeader()
	if err != nil {
		return err
	}
	r.numRecords = int64(binary.LittleEndian.Uint64(header[104:112]))
	r.metaInfoSize = int(binary.LittleEndian.Uint32(header[80:84]))
	err = r.loadMetaInfo()
	if err != nil {
		return err
	}
	r.record, err = yxrecord.FromFieldList(r.fields)
	if err != nil {
		return err
	}
	r.recordReader = bufrecord.NewBufferedRecordReader(
		r.stream,
		r.record.FixedSize,
		r.record.HasVar,
		r.numRecords,
	)
	return nil
}

func (r *r) getHeader() ([]byte, error) {
	headerBytes := make([]byte, 512)
	written, err := r.stream.Read(headerBytes)
	if written < 512 {
		return nil, err
	}
	return headerBytes, nil
}

func (r *r) loadMetaInfo() error {
	size := r.metaInfoSize * 2
	metaInfoBytes := make([]byte, size)
	read, err := r.stream.Read(metaInfoBytes)
	if err != nil {
		return err
	}
	if read < size {
		return errors.New(`not enough bytes read from meta-info`)
	}
	r.metaInfoStr = string(utf16.Decode(bytesToUint16(metaInfoBytes[0 : size-2])))
	return r.getFields()
}

func (r *r) getFields() error {
	var info metaInfo
	err := xml.Unmarshal([]byte(r.metaInfoStr), &info)
	if err != nil {
		return err
	}
	r.fields = info.Fields
	if len(r.fields) == 0 {
		r.fields = info.RecordInfoFields
	}
	return nil
}

func (r *r) close() {
	_ = r.stream.Close()
}

func bytesToUint16(buffer []byte) []uint16 {
	utf16Len := len(buffer) / 2
	var utf16Bytes []uint16
	rawHeader := (*reflect.SliceHeader)(unsafe.Pointer(&utf16Bytes))
	rawHeader.Data = uintptr(unsafe.Pointer(&buffer[0]))
	rawHeader.Len = utf16Len
	rawHeader.Cap = utf16Len
	return utf16Bytes
}
