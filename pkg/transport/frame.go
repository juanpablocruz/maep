package transport

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

const maxFrameSize = 1 << 20 // 1 MiB; tune as needed

func writeFrame(w io.Writer, p []byte) error {
	if len(p) > maxFrameSize {
		return fmt.Errorf("frame too large: %d > %d", len(p), maxFrameSize)
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(p)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(p)
	return err
}

func readFrame(r *bufio.Reader) ([]byte, error) {
	hdr, err := r.Peek(4)
	if err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr)
	if n > maxFrameSize {
		return nil, fmt.Errorf("frame too large: %d > %d", n, maxFrameSize)
	}
	_, _ = r.Discard(4)
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
