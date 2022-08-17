package yxdb_go

import (
	"github.com/tlarsendataguy-yxdb/yxdb-go/yxdb_record"
	"io"
	"os"
	"time"
)

type YxdbReader interface {
	io.Closer
	ListFields() []yxdb_record.YxdbField
	Next() bool
	NumRecords() int
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
	return &r{
		stream: file,
	}, nil
}

func ReadStream(stream io.ReadCloser) (YxdbReader, error) {
	return &r{
		stream: stream,
	}, nil
}

type r struct {
	stream io.ReadCloser
}

func (r *r) ListFields() []yxdb_record.YxdbField {
	//TODO implement me
	panic("implement me")
}

func (r *r) Close() error {
	return r.stream.Close()
}

func (r *r) Next() bool {
	//TODO implement me
	panic("implement me")
}

func (r *r) NumRecords() int {
	//TODO implement me
	panic("implement me")
}

func (r *r) MetaInfoStr() string {
	//TODO implement me
	panic("implement me")
}

func (r *r) ReadByteWithIndex(index int) (byte, bool) {
	//TODO implement me
	panic("implement me")
}

func (r *r) ReadByteWithName(name string) (byte, bool) {
	//TODO implement me
	panic("implement me")
}

func (r *r) ReadBoolWithIndex(index int) (bool, bool) {
	//TODO implement me
	panic("implement me")
}

func (r *r) ReadBoolWithName(name string) (bool, bool) {
	//TODO implement me
	panic("implement me")
}

func (r *r) ReadInt64WithIndex(index int) (int64, bool) {
	//TODO implement me
	panic("implement me")
}

func (r *r) ReadInt64WithName(name string) (int64, bool) {
	//TODO implement me
	panic("implement me")
}

func (r *r) ReadFloat64WithIndex(index int) (float64, bool) {
	//TODO implement me
	panic("implement me")
}

func (r *r) ReadFloat64WithName(name string) (float64, bool) {
	//TODO implement me
	panic("implement me")
}

func (r *r) ReadStringWithIndex(index int) (string, bool) {
	//TODO implement me
	panic("implement me")
}

func (r *r) ReadStringWithName(name string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

func (r *r) ReadTimeWithIndex(index int) (time.Time, bool) {
	//TODO implement me
	panic("implement me")
}

func (r *r) ReadTimeWithName(name string) (time.Time, bool) {
	//TODO implement me
	panic("implement me")
}

func (r *r) ReadBlobWithIndex(index int) []byte {
	//TODO implement me
	panic("implement me")
}

func (r *r) ReadBlobWithName(name string) []byte {
	//TODO implement me
	panic("implement me")
}
