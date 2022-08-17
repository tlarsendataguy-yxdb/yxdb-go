package buffered_record_reader_test

import (
	"encoding/binary"
	"fmt"
	r "github.com/tlarsendataguy-yxdb/yxdb-go/buffered_record_reader"
	"os"
	"testing"
)

func TestLotsOfRecords(t *testing.T) {
	reader := generateReader(getPath(`LotsOfRecords.yxdb`), 5, false)

	var recordsRead = 0
	for reader.NextRecord() {
		if reader.Err != nil {
			t.Fatalf(`expected no error but got: %v`, reader.Err.Error())
		}
		recordsRead++
		value := int(binary.LittleEndian.Uint32(reader.RecordBuffer[0:4]))
		if value != recordsRead {
			t.Fatalf(`expected %v but got %v`, recordsRead, value)
		}
	}
	if recordsRead != 100000 {
		t.Fatalf(`expected 100000 records but got %v`, recordsRead)
	}

	_ = reader.Close()
}

func TestVeryLongField(t *testing.T) {
	reader := generateReader(getPath(`VeryLongField.yxdb`), 6, true)

	var recordsRead byte = 0
	for reader.NextRecord() {
		if reader.Err != nil {
			t.Fatalf(`expected no error but got: %v`, reader.Err.Error())
		}
		recordsRead++
		value := reader.RecordBuffer[0]
		if value != recordsRead {
			t.Fatalf(`expected %v but got %v`, recordsRead, value)
		}
	}
	if recordsRead != 3 {
		t.Fatalf(`expected 3 records but got %v`, recordsRead)
	}
	_ = reader.Close()
}

func getPath(fileName string) string {
	return fmt.Sprintf(`../test_files/%v`, fileName)
}

func generateReader(path string, fixedLen int, hasVarFields bool) *r.BufferedRecordReader {
	stream, err := os.Open(path)
	if err != nil {
		panic(err.Error())
	}
	header := make([]byte, 512)
	read, err := stream.Read(header)
	if err != nil {
		panic(err.Error())
	}
	if read < 512 {
		panic("not enough bytes read")
	}
	metaInfoSize := int64(binary.LittleEndian.Uint32(header[80:84])) * 2
	totalRecords := int64(binary.LittleEndian.Uint64(header[104:112]))
	newOffset, err := stream.Seek(metaInfoSize, 1)
	if err != nil {
		panic(err.Error())
	}
	if newOffset != 512+metaInfoSize {
		panic("not enough bytes skipped in seek")
	}
	return r.NewBufferedRecordReader(stream, fixedLen, hasVarFields, totalRecords)
}
