package syncproto

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"

	bin "github.com/juanpablocruz/maep/pkg/internal/bin"
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/segment"
)

type Root struct {
	Hash [32]byte
}

func dbg(msg string, args ...any) {
	logger := slog.Default()
	if logger.Enabled(context.Background(), slog.LevelDebug) {
		logger.Debug(msg, args...)
	}
}

// legacy helpers retained for decoders below; new encoders use pkg/internal/bin
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

func putBytes(b *bytes.Buffer, p []byte) { _ = bin.PutBytes(b, p) }
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

// Summary marshalers
func MarshalSummary(s Summary) ([]byte, error) {
	var buf bytes.Buffer
	// Root first (32B)
	if _, err := buf.Write(s.Root[:]); err != nil {
		return nil, err
	}
	if err := bin.PutU32(&buf, uint32(len(s.Leaves))); err != nil {
		return nil, err
	}
	for _, lf := range s.Leaves {
		if err := bin.PutU16(&buf, uint16(len(lf.Key))); err != nil {
			return nil, err
		}
		if _, err := buf.WriteString(lf.Key); err != nil {
			return nil, err
		}
		if _, err := buf.Write(lf.Hash[:]); err != nil {
			return nil, err
		}
	}
	// Frontier (SV): count | repeated (keyLen|key | from(u32))
	if err := bin.PutU32(&buf, uint32(len(s.Frontier))); err != nil {
		return nil, err
	}
	for _, it := range s.Frontier {
		if err := bin.PutU16(&buf, uint16(len(it.Key))); err != nil {
			return nil, err
		}
		if _, err := buf.WriteString(it.Key); err != nil {
			return nil, err
		}
		if err := bin.PutU32(&buf, it.From); err != nil {
			return nil, err
		}
	}
	// SegFrontier: count | repeated (sid(u16) | 32B root | count(u32))
	if err := bin.PutU32(&buf, uint32(len(s.SegFrontier))); err != nil {
		return nil, err
	}
	for _, it := range s.SegFrontier {
		if err := bin.PutU16(&buf, uint16(it.SID)); err != nil {
			return nil, err
		}
		if _, err := buf.Write(it.Root[:]); err != nil {
			return nil, err
		}
		if err := bin.PutU32(&buf, it.Count); err != nil {
			return nil, err
		}
	}
	dbg("codec.encode_summary", "leaves", len(s.Leaves), "frontier", len(s.Frontier), "seg_frontier", len(s.SegFrontier), "bytes", buf.Len())
	return buf.Bytes(), nil
}
func EncodeSummary(s Summary) []byte { b, _ := MarshalSummary(s); return b }

// SummaryReq <-> bytes (just the 32B root for now)
func MarshalSummaryReq(r SummaryReq) ([]byte, error) { return MarshalRoot(Root{Hash: r.Root}) }
func EncodeSummaryReq(r SummaryReq) []byte           { b, _ := MarshalSummaryReq(r); return b }
func DecodeSummaryReq(b []byte) (SummaryReq, error) {
	rt, err := DecodeRoot(b)
	if err != nil {
		return SummaryReq{}, err
	}
	return SummaryReq{Root: rt.Hash}, nil
}

func DecodeSummary(b []byte) (Summary, error) {
	r := bytes.NewReader(b)
	var root [32]byte
	if _, err := io.ReadFull(r, root[:]); err != nil {
		slog.Warn("codec.decode_summary_err", "stage", "root", "err", err)
		return Summary{}, err
	}
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
	// Optional Frontier (SV). Backward-compatible: only parse if at least 4 bytes remain.
	var frontier []FrontierItem
	if r.Len() >= 4 {
		fn, err := getU32(r)
		if err != nil {
			slog.Warn("codec.decode_summary_err", "stage", "frontier.count", "err", err)
			return Summary{}, err
		}
		frontier = make([]FrontierItem, 0, fn)
		for range int(fn) {
			fkl, err := getU16(r)
			if err != nil {
				slog.Warn("codec.decode_summary_err", "stage", "frontier.keyLen", "err", err)
				return Summary{}, err
			}
			fk := make([]byte, fkl)
			if _, err := r.Read(fk); err != nil {
				slog.Warn("codec.decode_summary_err", "stage", "frontier.key", "err", err)
				return Summary{}, err
			}
			from, err := getU32(r)
			if err != nil {
				slog.Warn("codec.decode_summary_err", "stage", "frontier.from", "err", err)
				return Summary{}, err
			}
			frontier = append(frontier, FrontierItem{Key: string(fk), From: from})
		}
	}
	// SegFrontier
	var segFrontier []SegFrontierItem
	if r.Len() >= 4 {
		nseg, err := getU32(r)
		if err != nil {
			slog.Warn("codec.decode_summary_err", "stage", "segfrontier.count", "err", err)
			return Summary{}, err
		}
		segFrontier = make([]SegFrontierItem, 0, nseg)
		for range int(nseg) {
			sidU16, err := getU16(r)
			if err != nil {
				return Summary{}, err
			}
			var rootSeg [32]byte
			if _, err := io.ReadFull(r, rootSeg[:]); err != nil {
				return Summary{}, err
			}
			cnt, err := getU32(r)
			if err != nil {
				return Summary{}, err
			}
			segFrontier = append(segFrontier, SegFrontierItem{SID: segment.ID(sidU16), Root: rootSeg, Count: cnt})
		}
	}
	if r.Len() != 0 {
		slog.Warn("codec.decode_summary_trailing", "bytes", r.Len())
	}

	dbg("codec.decode_summary", "leaves", len(ls), "frontier", len(frontier), "seg_frontier", len(segFrontier), "bytes_in", len(b))
	return Summary{Root: root, Leaves: ls, Frontier: frontier, SegFrontier: segFrontier}, nil
}

// Req <-> bytes
func MarshalReq(q Req) ([]byte, error) {
	var buf bytes.Buffer
	if err := bin.PutU32(&buf, uint32(len(q.Needs))); err != nil {
		return nil, err
	}
	for _, n := range q.Needs {
		if err := bin.PutU16(&buf, uint16(len(n.Key))); err != nil {
			return nil, err
		}
		if _, err := buf.WriteString(n.Key); err != nil {
			return nil, err
		}
		if err := bin.PutU32(&buf, n.From); err != nil {
			return nil, err
		}
	}
	dbg("codec.encode_req", "needs", len(q.Needs), "bytes", buf.Len())
	return buf.Bytes(), nil
}
func EncodeReq(q Req) []byte { b, _ := MarshalReq(q); return b }

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
func MarshalDelta(d Delta) ([]byte, error) {
	var buf bytes.Buffer
	if err := bin.PutU32(&buf, uint32(len(d.Entries))); err != nil {
		return nil, err
	}
	totalOps := 0
	for _, e := range d.Entries {
		if err := bin.PutU16(&buf, uint16(len(e.Key))); err != nil {
			return nil, err
		}
		if _, err := buf.WriteString(e.Key); err != nil {
			return nil, err
		}
		if err := bin.PutU32(&buf, uint32(len(e.Ops))); err != nil {
			return nil, err
		}
		totalOps += len(e.Ops)
		for _, op := range e.Ops {
			if err := bin.PutU16(&buf, op.Version); err != nil {
				return nil, err
			}
			if err := buf.WriteByte(op.Kind); err != nil {
				return nil, err
			}
			if err := bin.PutU64(&buf, op.HLCTicks); err != nil {
				return nil, err
			}
			if err := bin.PutU64(&buf, uint64(op.WallNanos)); err != nil {
				return nil, err
			}
			if _, err := buf.Write(op.Actor[:]); err != nil {
				return nil, err
			}
			if _, err := buf.Write(op.Hash[:]); err != nil {
				return nil, err
			}
			if err := bin.PutBytes(&buf, op.Value); err != nil {
				return nil, err
			}
		}
	}
	dbg("codec.encode_delta", "entries", len(d.Entries), "ops", totalOps, "bytes", buf.Len())
	return buf.Bytes(), nil
}
func EncodeDelta(d Delta) []byte { b, _ := MarshalDelta(d); return b }

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
func MarshalSegAd(ad SegAd) ([]byte, error) {
	var buf bytes.Buffer
	if err := bin.PutU32(&buf, uint32(len(ad.Items))); err != nil {
		return nil, err
	}
	for _, it := range ad.Items {
		if err := bin.PutU16(&buf, uint16(it.SID)); err != nil {
			return nil, err
		}
		if _, err := buf.Write(it.Root[:]); err != nil {
			return nil, err
		}
		if err := bin.PutU32(&buf, it.Count); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}
func EncodeSegAd(ad SegAd) []byte { b, _ := MarshalSegAd(ad); return b }

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

// --- DeltaChunk ---
func MarshalDeltaChunk(c DeltaChunk) ([]byte, error) {
	var b bytes.Buffer
	if err := bin.PutU16(&b, uint16(c.SID)); err != nil {
		return nil, err
	}
	if err := bin.PutU32(&b, c.Seq); err != nil {
		return nil, err
	}
	if err := b.WriteByte(func() byte {
		if c.Last {
			return 1
		}
		return 0
	}()); err != nil {
		return nil, err
	}
	if err := bin.PutU32(&b, uint32(len(c.Entries))); err != nil {
		return nil, err
	}
	for _, e := range c.Entries {
		if err := bin.PutU16(&b, uint16(len(e.Key))); err != nil {
			return nil, err
		}
		if _, err := b.WriteString(e.Key); err != nil {
			return nil, err
		}
		if err := bin.PutU32(&b, uint32(len(e.Ops))); err != nil {
			return nil, err
		}
		for _, op := range e.Ops {
			if err := bin.PutU16(&b, op.Version); err != nil {
				return nil, err
			}
			if err := b.WriteByte(op.Kind); err != nil {
				return nil, err
			}
			if err := bin.PutU64(&b, op.HLCTicks); err != nil {
				return nil, err
			}
			if err := bin.PutU64(&b, uint64(op.WallNanos)); err != nil {
				return nil, err
			}
			if _, err := b.Write(op.Actor[:]); err != nil {
				return nil, err
			}
			if _, err := b.Write(op.Hash[:]); err != nil {
				return nil, err
			}
			if err := bin.PutBytes(&b, op.Value); err != nil {
				return nil, err
			}
		}
	}
	sum := sha256.Sum256(b.Bytes())
	var out bytes.Buffer
	if _, err := out.Write(b.Bytes()); err != nil {
		return nil, err
	}
	if _, err := out.Write(sum[:]); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
func EncodeDeltaChunk(c DeltaChunk) []byte { b, _ := MarshalDeltaChunk(c); return b }

func DecodeDeltaChunk(p []byte) (DeltaChunk, error) {
	if len(p) < 32 {
		return DeltaChunk{}, errors.New("delta chunk too short for hash")
	}

	payload := p[:len(p)-32]
	var got [32]byte
	copy(got[:], p[len(p)-32:])

	r := bytes.NewReader(payload)
	sidU16, err := getU16(r)
	if err != nil {
		return DeltaChunk{}, err
	}
	seq, err := getU32(r)
	if err != nil {
		return DeltaChunk{}, err
	}
	lastb, err := r.ReadByte()
	if err != nil {
		return DeltaChunk{Seq: seq}, err
	}
	nEnt, err := getU32(r)
	if err != nil {
		return DeltaChunk{Seq: seq}, err
	}
	ents := make([]DeltaEntry, 0, nEnt)
	for range int(nEnt) {
		kl, err := getU16(r)
		if err != nil {
			return DeltaChunk{}, err
		}
		kb := make([]byte, kl)
		if _, err := r.Read(kb); err != nil {
			return DeltaChunk{}, err
		}
		nOps, err := getU32(r)
		if err != nil {
			return DeltaChunk{}, err
		}
		ops := make([]model.Op, 0, nOps)
		for range int(nOps) {
			var op model.Op
			ver, err := getU16(r)
			if err != nil {
				return DeltaChunk{}, err
			}
			op.Version = ver
			kind, err := r.ReadByte()
			if err != nil {
				return DeltaChunk{}, err
			}
			op.Kind = kind
			op.HLCTicks, err = getU64(r)
			if err != nil {
				return DeltaChunk{}, err
			}
			w, err := getU64(r)
			if err != nil {
				return DeltaChunk{}, err
			}
			op.WallNanos = int64(w)
			if _, err := r.Read(op.Actor[:]); err != nil {
				return DeltaChunk{}, err
			}
			if _, err := r.Read(op.Hash[:]); err != nil {
				return DeltaChunk{}, err
			}
			val, err := getBytes(r)
			if err != nil {
				return DeltaChunk{Seq: seq}, err
			}
			op.Key = string(kb)
			op.Value = val
			ops = append(ops, op)
		}
		ents = append(ents, DeltaEntry{Key: string(kb), Ops: ops})
	}
	if r.Len() != 0 {
		dbg("decode_delta_chunk_trailing", "bytes", r.Len())
	}

	c := DeltaChunk{SID: segment.ID(sidU16), Seq: seq, Last: lastb == 1, Entries: ents}
	want := sha256.Sum256(payload)
	c.Hash = got
	if got != want {
		return c, errors.New("bad chunk hash")
	}
	return c, nil
}

// --- Ack ---
func MarshalAck(a Ack) ([]byte, error) {
	var b bytes.Buffer
	if err := bin.PutU16(&b, uint16(a.SID)); err != nil {
		return nil, err
	}
	if err := bin.PutU32(&b, a.Seq); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
func EncodeAck(a Ack) []byte { b, _ := MarshalAck(a); return b }

func DecodeAck(p []byte) (Ack, error) {
	r := bytes.NewReader(p)
	sidU16, err := getU16(r)
	if err != nil {
		return Ack{}, err
	}
	seq, err := getU32(r)
	if err != nil {
		return Ack{}, err
	}
	if r.Len() != 0 {
		dbg("decode_ack_trailing", "bytes", r.Len())
	}
	return Ack{SID: segment.ID(sidU16), Seq: seq}, nil
}

func MarshalRoot(r Root) ([]byte, error) { b := make([]byte, 32); copy(b, r.Hash[:]); return b, nil }
func EncodeRoot(r Root) []byte           { b, _ := MarshalRoot(r); return b }
func DecodeRoot(b []byte) (Root, error) {
	if len(b) < 32 {
		return Root{}, errors.New("short root")
	}
	var r Root
	copy(r.Hash[:], b[:32])
	return r, nil
}

func MarshalDeltaNack(n DeltaNack) ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, n.Seq); err != nil {
		return nil, err
	}
	if err := buf.WriteByte(n.Code); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func EncodeDeltaNack(n DeltaNack) []byte { b, _ := MarshalDeltaNack(n); return b }

func DecodeDeltaNack(b []byte) (DeltaNack, error) {
	if len(b) < 5 {
		return DeltaNack{}, errors.New("short nack")
	}
	n := DeltaNack{
		Seq:  binary.BigEndian.Uint32(b[:4]),
		Code: b[4],
	}
	if len(b) != 5 {
		return DeltaNack{}, errors.New("trailing bytes in nack")
	}
	return n, nil
}
