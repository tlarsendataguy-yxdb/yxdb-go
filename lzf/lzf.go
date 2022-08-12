package lzf

type Lzf struct {
	InBuffer  []byte
	OutBuffer []byte
	inIndex   int
	outIndex  int
	inLen     int
}

func (l *Lzf) Decompress(length int) int {
	l.inLen = length
	l.reset()

	if l.inLen == 0 {
		return 0
	}

	for l.inIndex < l.inLen {
		ctrl := l.InBuffer[l.inIndex]
		l.inIndex++

		if ctrl < 32 {
			l.copyByteSequence(ctrl)
		} else {
			l.expandRepeatedBytes(ctrl)
		}
	}
	return l.outIndex
}

func (l *Lzf) reset() {
	l.inIndex = 0
	l.outIndex = 0
}

func (l *Lzf) copyByteSequence(ctrl byte) {
	length := int(ctrl + 1)
	if l.outIndex+length > len(l.OutBuffer) {
		panic("output array is too small")
	}
	copy(l.OutBuffer[l.outIndex:l.outIndex+length], l.InBuffer[l.inIndex:l.inIndex+length])
	l.inIndex += length
	l.outIndex += length
}

func (l *Lzf) expandRepeatedBytes(ctrl byte) {
	length := int(ctrl >> 5)
	reference := l.outIndex - (int(ctrl&0x1f) << 8) - 1

	if length == 7 {
		length += int(l.InBuffer[l.inIndex])
		l.inIndex++
	}

	if l.outIndex+length+2 > len(l.OutBuffer) {
		panic("output array is too small")
	}

	reference -= int(l.InBuffer[l.inIndex])
	l.inIndex++

	length += 2

	for length > 0 {
		size := min(length, l.outIndex-reference)
		reference = l.copyFromReferenceAndIncrement(reference, size)
		length -= size
	}
}

func (l *Lzf) copyFromReferenceAndIncrement(reference int, size int) int {
	copy(l.OutBuffer[l.outIndex:l.outIndex+size], l.OutBuffer[reference:reference+size])
	l.outIndex += size
	return reference + size
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
