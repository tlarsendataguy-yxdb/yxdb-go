package buffered_record_reader

import (
	"encoding/binary"
	"fmt"
	l "github.com/tlarsendataguy-yxdb/yxdb-go/lzf"
	"io"
)

const lzfBufferSize = 262144

type BufferedRecordReader struct {
	RecordBuffer      []byte
	Err               error
	recordBufferIndex int
	totalRecords      int64
	stream            io.Reader
	fixedLen          int
	hasVarFields      bool
	lzfIn             []byte
	lzfOut            []byte
	lzf               l.Lzf
	lzfLengthBuffer   []byte
	lzfOutIndex       int
	lzfOutSize        int
	currentRecord     int64
}

func NewBufferedRecordReader(stream io.Reader, fixedLen int, hasVarFields bool, totalRecords int64) *BufferedRecordReader {
	var recordBuffer []byte
	if hasVarFields {
		recordBuffer = make([]byte, fixedLen+4+1000)
	} else {
		recordBuffer = make([]byte, fixedLen)
	}
	lzfIn := make([]byte, lzfBufferSize)
	lzfOut := make([]byte, lzfBufferSize)
	lzf := l.Lzf{InBuffer: lzfIn, OutBuffer: lzfOut}
	reader := &BufferedRecordReader{
		RecordBuffer:      recordBuffer,
		recordBufferIndex: 0,
		totalRecords:      totalRecords,
		stream:            stream,
		fixedLen:          fixedLen,
		hasVarFields:      hasVarFields,
		lzfIn:             lzfIn,
		lzfOut:            lzfOut,
		lzf:               lzf,
		lzfLengthBuffer:   make([]byte, 4),
		lzfOutIndex:       0,
		lzfOutSize:        0,
		currentRecord:     0,
	}
	return reader
}

func (r *BufferedRecordReader) NextRecord() bool {
	r.currentRecord++
	if r.currentRecord > r.totalRecords {
		return false
	}

	r.recordBufferIndex = 0
	var err error
	if r.hasVarFields {
		err = r.readVariableRecord()
	} else {
		err = r.read(r.fixedLen)
	}
	if err != nil {
		r.Err = err
		return false
	}
	return true
}

func (r *BufferedRecordReader) readVariableRecord() error {
	err := r.read(r.fixedLen + 4)
	if err != nil {
		return err
	}
	varLength := int(binary.LittleEndian.Uint32(r.RecordBuffer[r.recordBufferIndex-4 : r.recordBufferIndex]))
	if r.fixedLen+varLength+4 > cap(r.RecordBuffer) {
		newLength := (r.fixedLen + 4 + varLength) * 2
		newBuffer := make([]byte, newLength)
		copyTo := r.fixedLen + 4
		copySlice(r.RecordBuffer, 0, newBuffer, 0, copyTo)
		r.RecordBuffer = newBuffer
	}
	return r.read(varLength)
}

func (r *BufferedRecordReader) read(size int) error {
	var err error
	for size > 0 {
		if r.lzfOutSize == 0 {
			r.lzfOutSize, err = r.readNextLzfBlock()
			if err != nil {
				return err
			}
		}

		for size+r.lzfOutIndex > r.lzfOutSize {
			size -= r.copyRemainingLzfOutToRecord()
			r.lzfOutSize, err = r.readNextLzfBlock()
			if err != nil {
				return err
			}
			r.lzfOutIndex = 0
		}

		lenToCopy := min(r.lzfOutSize, size)
		copySlice(r.lzfOut, r.lzfOutIndex, r.RecordBuffer, r.recordBufferIndex, lenToCopy)
		r.lzfOutIndex += lenToCopy
		r.recordBufferIndex += lenToCopy
		size -= lenToCopy
	}
	return nil
}

func (r *BufferedRecordReader) copyRemainingLzfOutToRecord() int {
	var remainingLzf = r.lzfOutSize - r.lzfOutIndex
	copySlice(r.lzfOut, r.lzfOutIndex, r.RecordBuffer, r.recordBufferIndex, remainingLzf)
	r.recordBufferIndex += remainingLzf
	return remainingLzf
}

func (r *BufferedRecordReader) readNextLzfBlock() (int, error) {
	lzfBlockLength, err := r.readLzfBlockLength()
	if err != nil {
		return 0, err
	}
	checkbit := lzfBlockLength & 0x80000000
	if checkbit > 0 {
		lzfBlockLength &= 0x7fffffff
		return r.stream.Read(r.lzfOut[0:lzfBlockLength])
	}
	readIn, err := r.stream.Read(r.lzfIn[0:lzfBlockLength])
	if err != nil {
		return readIn, err
	}
	return r.lzf.Decompress(readIn), nil
}

func (r *BufferedRecordReader) readLzfBlockLength() (int, error) {
	read, err := r.stream.Read(r.lzfLengthBuffer)
	if read < 4 {
		return read, fmt.Errorf("yxdb file is not valid")
	}
	blockLength := int(binary.LittleEndian.Uint32(r.lzfLengthBuffer))
	return blockLength, err
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
func copySlice(src []byte, srcIndex int, dest []byte, destIndex int, size int) {
	copy(dest[destIndex:destIndex+size], src[srcIndex:srcIndex+size])
}