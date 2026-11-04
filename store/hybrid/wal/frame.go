package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	headerLen = 8
	maxRecord = 1 << 30
)

var table = crc32.MakeTable(crc32.Castagnoli)

// ErrTorn means a frame was cut short or failed its checksum, as happens to the
// last frame when the process dies mid-write.
var ErrTorn = errors.New("wal: torn or corrupt frame")

// AppendFrame appends rec to b as a length and checksum header followed by rec.
func AppendFrame(b, rec []byte) []byte {
	var h [headerLen]byte
	binary.LittleEndian.PutUint32(h[:4], uint32(len(rec)))
	binary.LittleEndian.PutUint32(h[4:], crc32.Checksum(rec, table))
	return append(append(b, h[:]...), rec...)
}

// ReadFrames calls fn with each record in r and returns how many bytes held whole
// frames. It returns ErrTorn at the first bad frame.
func ReadFrames(r io.Reader, fn func(rec []byte) error) (int64, error) {
	br := bufio.NewReaderSize(r, 1<<20)
	var good int64
	var h [headerLen]byte
	var rec []byte
	for {
		if _, err := io.ReadFull(br, h[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return good, nil
			}
			return good, ErrTorn
		}
		n := binary.LittleEndian.Uint32(h[:4])
		if n > maxRecord {
			return good, ErrTorn
		}
		if cap(rec) < int(n) {
			rec = make([]byte, n)
		}
		rec = rec[:n]
		if _, err := io.ReadFull(br, rec); err != nil {
			return good, ErrTorn
		}
		if crc32.Checksum(rec, table) != binary.LittleEndian.Uint32(h[4:]) {
			return good, ErrTorn
		}
		if err := fn(rec); err != nil {
			return good, fmt.Errorf("wal: applying record at %d: %w", good, err)
		}
		good += int64(headerLen + n)
	}
}
