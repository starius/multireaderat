package multireaderat

// This code is based on https://github.com/mg-rast/shock/blob/v0.9.29/shock-server/node/file/file.go#L89

import (
	"fmt"
	"io"
)

// multifd contains file boundary information
type multifd struct {
	start int64
	end   int64
	size  int64
}

// MultiReaderAt is private struct for the multi-file ReaderAt
// that provides the ablity to use indexes with vitrual files.
type MultiReaderAt struct {
	readers    []io.ReaderAt
	boundaries []multifd
	size       int64
}

// New returns a MultiReaderAt that's the logical concatenation of
// the provided input readers.
func New(readers []io.ReaderAt, lengths []int64) (*MultiReaderAt, error) {
	if len(readers) != len(lengths) {
		return nil, fmt.Errorf("the number of io.ReaderAt's must be equal to the number of lengths (%d != %d)", len(readers), len(lengths))
	}

	mr := &MultiReaderAt{
		readers:    readers,
		boundaries: make([]multifd, len(lengths)),
	}

	start := int64(0)
	for i, length := range lengths {
		if length <= 0 {
			return nil, fmt.Errorf("each length must be > 0, length of %d-th reader is %d", i, length)
		}
		mr.boundaries[i] = multifd{
			start: start,
			end:   start + length,
			size:  length,
		}
		start += length
	}
	mr.size = start

	return mr, nil
}

// ReadAt is the magic sauce. Heavily commented to include all logic.
func (mr *MultiReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	startF, endF := 0, 0
	startPos, endPos, length := int64(0), int64(0), int64(len(p))

	if off > mr.size {
		return 0, io.EOF
	}

	if len(p) == 0 {
		return 0, nil
	}

	// find start
	for i, fd := range mr.boundaries {
		if off >= fd.start && off <= fd.end {
			startF = i
			startPos = off - fd.start
			break
		}
	}

	// find end
	if off+length > mr.size {
		endF = len(mr.readers) - 1
		endPos = mr.size - mr.boundaries[endF].start
	} else {
		for i, fd := range mr.boundaries {
			if off+length >= fd.start && off+length <= fd.end {
				endF = i
				endPos = off + length - fd.start
				break
			}
		}
	}

	if startF == endF {
		// read startpos till endpos
		// println("--> readat: startpos till endpos")
		// fmt.Printf("file: %d, offset: %d, length: %d\n", startF, startPos, endPos-startPos)
		return mr.readers[startF].ReadAt(p[0:length], startPos)
	} else {
		buffPos := 0
		for i := startF; i <= endF; i++ {
			if i == startF {
				// read startpos till end of file
				// println("--> readat: startpos till end of file")
				// fmt.Printf("file: %d, offset: %d, length: %d, buffPos: %d\n", i, startPos, mr.boundaries[i].size-startPos, buffPos)
				if rn, err := mr.readers[i].ReadAt(p[buffPos:buffPos+int(mr.boundaries[i].size-startPos)], startPos); err != nil && err != io.EOF {
					return 0, err
				} else {
					buffPos = buffPos + int(mr.boundaries[i].size-startPos)
					n = n + rn
				}
			} else if i == endF {
				// read start of file till endpos
				// println("--> readat: start of file till endpos")
				// fmt.Printf("file: %d, offset: %d, length: %d, buffPos: %d\n", i, 0, endPos, buffPos)
				if rn, err := mr.readers[i].ReadAt(p[buffPos:buffPos+int(endPos)], 0); err != nil && err != io.EOF {
					println("--> error here: ", err.Error())
					return 0, err
				} else {
					buffPos = buffPos + int(endPos)
					n = n + rn
				}
			} else {
				// read entire file
				// println("--> readat: entire file")
				// fmt.Printf("file: %d, offset: %d, length: %d, buffPos: %d\n", i, 0, mr.boundaries[i].size, buffPos)
				if rn, err := mr.readers[i].ReadAt(p[buffPos:buffPos+int(mr.boundaries[i].size)], 0); err != nil && err != io.EOF {
					return 0, err
				} else {
					buffPos = buffPos + int(mr.boundaries[i].size)
					n = n + rn
				}
			}
		}
	}
	if n < int(length) {
		return n, io.EOF
	}
	return
}
