package syncproto

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"

	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/segment"
)

func dbg(msg string, args ...any) {
	logger := slog.Default()
	if logger.Enabled(context.Background(), slog.LevelDebug) {
		logger.Debug(msg, args...)
	}
}
func putU16(b *bytes.Buffer, v uint16) { _ = binary.Write(b, binary.BigEndian, v) }
func putU32(b *bytes.Buffer, v uint32) { _ = binary.Write(b, binary.BigEndian, v) }
func putU64(b *bytes.Buffer, v uint64) { _ = binary.Write(b, binary.BigEndian, v) }
func getU16(r *bytes.Reader) (uint16, error) {
	var v uint16
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}
func getU32(r *bytes.Reader) (uint32, error) {
	var v uint32
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}
func getU64(r *bytes.Reader) (uint64, error) {
	var v uint64
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

func putBytes(b *bytes.Buffer, p []byte) { putU32(b, uint32(len(p))); b.Write(p) }
func getBytes(r *bytes.Reader) ([]byte, error) {
	n32, err := getU32(r)
	if err != nil {
		return nil, err
	}
	// 1) Must not exceed remaining bytes
	if uint64(n32) > uint64(r.Len()) {
		return nil, io.ErrUnexpectedEOF
	}
	// 2) Must fit into int on this platform
	maxInt := int(^uint(0) >> 1) // platform-dependent MaxInt
	if uint64(n32) > uint64(maxInt) {
		return nil, errors.New("length too large")
	}

	n := int(n32)
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// Summary <-> bytes
func EncodeSummary(s Summary) []byte {
	var buf bytes.Buffer
	putU32(&buf, uint32(len(s.Leaves)))
	for _, lf := range s.Leaves {
		putU16(&buf, uint16(len(lf.Key)))
		buf.WriteString(lf.Key)
		buf.Write(lf.Hash[:])
	}
	dbg("codec.encode_summary", "leaves", len(s.Leaves), "bytes", buf.Len())
	return buf.Bytes()
}

func DecodeSummary(b []byte) (Summary, error) {
	r := bytes.NewReader(b)
	n, err := getU32(r)
	if err != nil {
		slog.Warn("codec.decode_summary_err", "stage", "count", "err", err)
		return Summary{}, err
	}
	ls := make([]LeafSummary, 0, n)
	for range int(n) {
		kl, err := getU16(r)
		if err != nil {
			slog.Warn("codec.decode_summary_err", "stage", "keyLen", "err", err)
			return Summary{}, err
		}
		k := make([]byte, kl)
		if _, err := r.Read(k); err != nil {
			slog.Warn("codec.decode_summary_err", "stage", "key", "err", err)
			return Summary{}, err
		}
		var h [32]byte
		if _, err := r.Read(h[:]); err != nil {
			slog.Warn("codec.decode_summary_err", "stage", "hash", "err", err)
			return Summary{}, err
		}
		ls = append(ls, LeafSummary{Key: string(k), Hash: h})
	}
	if r.Len() != 0 {
		slog.Warn("codec.decode_summary_trailing", "bytes", r.Len())
	}

	dbg("codec.decode_summary", "leaves", len(ls), "bytes_in", len(b))
	return Summary{Leaves: ls}, nil
}

// Req <-> bytes
func EncodeReq(q Req) []byte {
	var buf bytes.Buffer
	putU32(&buf, uint32(len(q.Needs)))
	for _, n := range q.Needs {
		putU16(&buf, uint16(len(n.Key)))
		buf.WriteString(n.Key)
		putU32(&buf, n.From)
	}
	dbg("codec.encode_req", "needs", len(q.Needs), "bytes", buf.Len())
	return buf.Bytes()
}

func DecodeReq(b []byte) (Req, error) {
	r := bytes.NewReader(b)
	n, err := getU32(r)
	if err != nil {
		slog.Warn("codec.decode_req_err", "stage", "count", "err", err)
		return Req{}, err
	}
	needs := make([]Need, 0, n)
	for range int(n) {
		kl, err := getU16(r)
		if err != nil {
			slog.Warn("codec.decode_req_err", "stage", "keyLen", "err", err)
			return Req{}, err
		}
		kb := make([]byte, kl)
		if _, err := r.Read(kb); err != nil {
			slog.Warn("codec.decode_req_err", "stage", "key", "err", err)
			return Req{}, err
		}
		from, err := getU32(r)
		if err != nil {
			slog.Warn("codec.decode_req_err", "stage", "from", "err", err)
			return Req{}, err
		}
		needs = append(needs, Need{Key: string(kb), From: from})
	}
	if r.Len() != 0 {
		slog.Warn("codec.decode_req_trailing", "bytes", r.Len())
	}
	dbg("codec.decode_req", "needs", len(needs), "bytes_in", len(b))
	return Req{Needs: needs}, nil
}

// Delta <-> bytes
// Entry: keyLen|key | count(u32) | repeated Op
// Op: version(u16) kind(u8) | hlc(u64) wall(u64) | actor(16) hash(32) | vlen(u32)|value
func EncodeDelta(d Delta) []byte {
	var buf bytes.Buffer
	putU32(&buf, uint32(len(d.Entries)))
	totalOps := 0
	for _, e := range d.Entries {
		putU16(&buf, uint16(len(e.Key)))
		buf.WriteString(e.Key)
		putU32(&buf, uint32(len(e.Ops)))
		totalOps += len(e.Ops)
		for _, op := range e.Ops {
			putU16(&buf, op.Version)
			buf.WriteByte(op.Kind)
			putU64(&buf, op.HLCTicks)
			putU64(&buf, uint64(op.WallNanos))
			buf.Write(op.Actor[:])
			buf.Write(op.Hash[:])
			putBytes(&buf, op.Value)
		}
	}
	dbg("codec.encode_delta", "entries", len(d.Entries), "ops", totalOps, "bytes", buf.Len())
	return buf.Bytes()
}

func DecodeDelta(b []byte) (Delta, error) {
	r := bytes.NewReader(b)
	n, err := getU32(r)
	if err != nil {
		slog.Warn("codec.decode_delta_err", "stage", "entryCount", "err", err)
		return Delta{}, err
	}
	out := make([]DeltaEntry, 0, n)
	totalOps := 0
	for range int(n) {
		kl, err := getU16(r)
		if err != nil {
			slog.Warn("codec.decode_delta_err", "stage", "keyLen", "err", err)
			return Delta{}, err
		}
		k := make([]byte, kl)
		if _, err := r.Read(k); err != nil {
			slog.Warn("codec.decode_delta_err", "stage", "key", "err", err)
			return Delta{}, err
		}
		cnt, err := getU32(r)
		if err != nil {
			slog.Warn("codec.decode_delta_err", "stage", "opCount", "err", err)
			return Delta{}, err
		}
		ops := make([]model.Op, 0, cnt)
		for range int(cnt) {
			var op model.Op
			if op.Version, err = getU16(r); err != nil {
				slog.Warn("codec.decode_delta_err", "stage", "op.version", "err", err)
				return Delta{}, err
			}
			bk, err := r.ReadByte()
			if err != nil {
				slog.Warn("codec.decode_delta_err", "stage", "op.kind", "err", err)
				return Delta{}, err
			}
			op.Kind = bk
			if op.HLCTicks, err = getU64(r); err != nil {
				slog.Warn("codec.decode_delta_err", "stage", "op.hlc", "err", err)
				return Delta{}, err
			}
			w, err := getU64(r)
			if err != nil {
				slog.Warn("codec.decode_delta_err", "stage", "op.wall", "err", err)
				return Delta{}, err
			}
			op.WallNanos = int64(w)
			if _, err := r.Read(op.Actor[:]); err != nil {
				slog.Warn("codec.decode_delta_err", "stage", "op.actor", "err", err)
				return Delta{}, err
			}
			if _, err := r.Read(op.Hash[:]); err != nil {
				slog.Warn("codec.decode_delta_err", "stage", "op.hash", "err", err)
				return Delta{}, err
			}
			val, err := getBytes(r)
			if err != nil {
				slog.Warn("codec.decode_delta_err", "stage", "op.value", "err", err)
				return Delta{}, err
			}
			op.Key = string(k) // entry key applies to all ops here
			op.Value = val
			ops = append(ops, op)
			totalOps++
		}
		out = append(out, DeltaEntry{Key: string(k), Ops: ops})
	}
	// Check for trailing junk
	if r.Len() != 0 {
		slog.Warn("codec.decode_delta_trailing", "bytes", r.Len())
	}
	dbg("codec.decode_delta", "entries", len(out), "ops", totalOps, "bytes_in", len(b))

	return Delta{Entries: out}, nil
}

// SegAd <-> bytes
// wire format: u32 count | repeated (u16 sid | 32B root | u32 keyCount)
func EncodeSegAd(ad SegAd) []byte {
	var buf bytes.Buffer
	putU32(&buf, uint32(len(ad.Items)))
	for _, it := range ad.Items {
		putU16(&buf, uint16(it.SID))
		buf.Write(it.Root[:])
		putU32(&buf, it.Count)
	}
	return buf.Bytes()
}

func DecodeSegAd(b []byte) (SegAd, error) {
	r := bytes.NewReader(b)
	n, err := getU32(r)
	if err != nil {
		return SegAd{}, err
	}
	items := make([]SegAdItem, 0, n)
	for range int(n) {
		sidU16, err := getU16(r)
		if err != nil {
			return SegAd{}, err
		}
		var root [32]byte
		if _, err := io.ReadFull(r, root[:]); err != nil {
			return SegAd{}, err
		}
		cnt, err := getU32(r)
		if err != nil {
			return SegAd{}, err
		}
		items = append(items, SegAdItem{SID: segment.ID(sidU16), Root: root, Count: cnt})
	}
	if r.Len() != 0 {
		return SegAd{}, errors.New("trailing bytes in segad")
	}
	return SegAd{Items: items}, nil
}
