package extractors

import (
	"encoding/binary"
	"math"
	"reflect"
	"strconv"
	"time"
	"unicode/utf16"
	"unsafe"
)

const dateFormat = `2006-01-02`
const dateTimeFormat = `2006-01-02 15:04:05`

type BoolExtractor func([]byte) (bool, bool)
type ByteExtractor func([]byte) (byte, bool)
type Int64Extractor func([]byte) (int64, bool)
type Float64Extractor func([]byte) (float64, bool)
type TimeExtractor func([]byte) (time.Time, bool)
type StringExtractor func([]byte) (string, bool)
type BlobExtractor func([]byte) []byte

func NewBoolExtractor(start int) BoolExtractor {
	return func(buffer []byte) (bool, bool) {
		value := buffer[start]
		if value == 2 {
			return false, true
		}
		return value == 1, false
	}
}

func NewByteExtractor(start int) ByteExtractor {
	return func(buffer []byte) (byte, bool) {
		if buffer[start+1] == 1 {
			return 0, true
		}
		return buffer[start], false
	}
}

func NewInt16Extractor(start int) Int64Extractor {
	return func(buffer []byte) (int64, bool) {
		if buffer[start+2] == 1 {
			return 0, true
		}
		return int64(binary.LittleEndian.Uint16(buffer[start : start+2])), false
	}
}

func NewInt32Extractor(start int) Int64Extractor {
	return func(buffer []byte) (int64, bool) {
		if buffer[start+4] == 1 {
			return 0, true
		}
		return int64(binary.LittleEndian.Uint32(buffer[start : start+4])), false
	}
}

func NewInt64Extractor(start int) Int64Extractor {
	return func(buffer []byte) (int64, bool) {
		if buffer[start+8] == 1 {
			return 0, true
		}
		return int64(binary.LittleEndian.Uint64(buffer[start : start+8])), false
	}
}

func NewFixedDecimalExtractor(start int, fieldLength int) Float64Extractor {
	return func(buffer []byte) (float64, bool) {
		if buffer[start+fieldLength] == 1 {
			return 0.0, true
		}
		str := getString(buffer, start, fieldLength, 1)
		result, _ := strconv.ParseFloat(str, 64)
		return result, false
	}
}

func NewFloatExtractor(start int) Float64Extractor {
	return func(buffer []byte) (float64, bool) {
		if buffer[start+4] == 1 {
			return 0.0, true
		}
		return float64(math.Float32frombits(binary.LittleEndian.Uint32(buffer[start : start+4]))), false
	}
}

func NewDoubleExtractor(start int) Float64Extractor {
	return func(buffer []byte) (float64, bool) {
		if buffer[start+8] == 1 {
			return 0.0, true
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(buffer[start : start+8])), false
	}
}

func NewDateExtractor(start int) TimeExtractor {
	return func(buffer []byte) (time.Time, bool) {
		if buffer[start+10] == 1 {
			return time.Time{}, true
		}
		value, _ := time.Parse(dateFormat, string(buffer[start:start+10]))
		return value, false
	}
}

func NewDateTimeExtractor(start int) TimeExtractor {
	return func(buffer []byte) (time.Time, bool) {
		if buffer[start+19] == 1 {
			return time.Time{}, true
		}
		value, _ := time.Parse(dateTimeFormat, string(buffer[start:start+19]))
		return value, false
	}
}

func NewStringExtractor(start int, fieldLength int) StringExtractor {
	return func(buffer []byte) (string, bool) {
		if buffer[start+fieldLength] == 1 {
			return ``, true
		}
		return getString(buffer, start, fieldLength, 1), false
	}
}

func NewWStringExtractor(start int, fieldLength int) StringExtractor {
	return func(buffer []byte) (string, bool) {
		if buffer[start+(fieldLength*2)] == 1 {
			return ``, true
		}
		return getString(buffer, start, fieldLength, 2), false
	}
}

func NewV_StringExtractor(start int) StringExtractor {
	return func(buffer []byte) (string, bool) {
		bytes := parseBlob(buffer, start)
		if bytes == nil {
			return ``, true
		}
		return string(bytes), false
	}
}

func NewV_WStringExtractor(start int) StringExtractor {
	return func(buffer []byte) (string, bool) {
		bytes := parseBlob(buffer, start)
		if bytes == nil {
			return ``, true
		}
		if len(bytes) == 0 {
			return ``, false
		}
		return string(utf16.Decode(bytesToUint16(bytes))), false
	}
}

func NewBlobExtractor(start int) BlobExtractor {
	return func(buffer []byte) []byte {
		return parseBlob(buffer, start)
	}
}

func getString(buffer []byte, start int, fieldLength int, charSize int) string {
	length := getStringLen(buffer, start, fieldLength, charSize)
	if length == 0 {
		return ``
	}

	if charSize == 1 {
		end := start + length
		return string(buffer[start:end])
	}
	end := start + (length * 2)
	return string(utf16.Decode(bytesToUint16(buffer[start:end])))
}

func getStringLen(buffer []byte, start int, fieldLength int, charSize int) int {
	fieldTo := start + (fieldLength * charSize)
	strLen := 0
	for i := start; i < fieldTo; i += charSize {
		if buffer[i] == 0 && buffer[i+(charSize-1)] == 0 {
			break
		}
		strLen++
	}
	return strLen
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

func parseBlob(buffer []byte, start int) []byte {
	fixedPortion := int(binary.LittleEndian.Uint32(buffer[start : start+4]))
	if fixedPortion == 0 {
		return []byte{}
	}
	if fixedPortion == 1 {
		return nil
	}
	if isTiny(fixedPortion) {
		return getTinyBlob(start, buffer)
	}

	blockStart := start + (fixedPortion & 0x7fffffff)
	blockFirstByte := buffer[blockStart]
	if isSmallBlock(blockFirstByte) {
		return getSmallBlob(buffer, blockStart)
	}
	return getNormalBlob(buffer, blockStart)
}

func isTiny(fixedPortion int) bool {
	bitCheck1 := fixedPortion & 0x80000000
	bitCheck2 := fixedPortion & 0x30000000
	return bitCheck1 == 0 && bitCheck2 != 0
}

func getTinyBlob(start int, buffer []byte) []byte {
	intVal := int(binary.LittleEndian.Uint32(buffer[start : start+4]))
	length := intVal >> 28
	blob := make([]byte, length)
	copy(blob, buffer[start:start+length])
	return blob
}

func isSmallBlock(value byte) bool {
	return (value & 1) == 1
}

func getSmallBlob(buffer []byte, blockStart int) []byte {
	blockFirstByte := buffer[blockStart]
	blobLen := int(blockFirstByte >> 1)
	blobStart := blockStart + 1
	blob := make([]byte, blobLen)
	copy(blob, buffer[blobStart:blobStart+blobLen])
	return blob
}

func getNormalBlob(buffer []byte, blockStart int) []byte {
	blobLen := int(binary.LittleEndian.Uint32(buffer[blockStart:blockStart+4])) / 2
	blobStart := blockStart + 4
	blob := make([]byte, blobLen)
	copy(blob, buffer[blobStart:blobStart+blobLen])
	return blob
}
