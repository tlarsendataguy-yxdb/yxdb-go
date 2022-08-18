package yxdb_go

import (
	"encoding/binary"
	"encoding/xml"
	"errors"
	"github.com/tlarsendataguy-yxdb/yxdb-go/buffered_record_reader"
	"github.com/tlarsendataguy-yxdb/yxdb-go/meta_info_field"
	"github.com/tlarsendataguy-yxdb/yxdb-go/yxdb_record"
	"io"
	"os"
	"reflect"
	"time"
	"unicode/utf16"
	"unsafe"
)

type metaInfo struct {
	Fields           []meta_info_field.MetaInfoField `xml:"Field"`
	RecordInfoFields []meta_info_field.MetaInfoField `xml:"RecordInfo>Field"`
}

type YxdbReader interface {
	io.Closer
	ListFields() []yxdb_record.YxdbField
	Next() bool
	NumRecords() int64
	MetaInfoStr() string
	ReadByteWithIndex(int) (byte, bool)
	ReadByteWithName(string) (byte, bool)
	ReadBoolWithIndex(int) (bool, bool)
	ReadBoolWithName(string) (bool, bool)
	ReadInt64WithIndex(int) (int64, bool)
	ReadInt64WithName(string) (int64, bool)
	ReadFloat64WithIndex(int) (float64, bool)
	ReadFloat64WithName(string) (float64, bool)
	ReadStringWithIndex(int) (string, bool)
	ReadStringWithName(string) (string, bool)
	ReadTimeWithIndex(int) (time.Time, bool)
	ReadTimeWithName(string) (time.Time, bool)
	ReadBlobWithIndex(int) []byte
	ReadBlobWithName(string) []byte
}

func ReadFile(path string) (YxdbReader, error) {
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

func ReadStream(stream io.ReadCloser) (YxdbReader, error) {
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
	fields       []meta_info_field.MetaInfoField
	metaInfoSize int
	numRecords   int64
	record       *yxdb_record.YxdbRecord
	recordReader *buffered_record_reader.BufferedRecordReader
	metaInfoStr  string
}

func (r *r) ListFields() []yxdb_record.YxdbField {
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
	r.fields = make([]meta_info_field.MetaInfoField, 0)
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
	r.record, err = yxdb_record.FromFieldList(r.fields)
	if err != nil {
		return err
	}
	r.recordReader = buffered_record_reader.NewBufferedRecordReader(
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
