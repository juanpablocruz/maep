package syncproto

import (
	"bytes"
	"errors"
	"io"
	"sort"

	"github.com/juanpablocruz/maep/pkg/materialize"
	"github.com/juanpablocruz/maep/pkg/segment"
)

type SegKeysReq struct {
	SIDs []segment.ID
}

type SegKeys struct {
	Items []SegKeysItem
}

type SegKeysItem struct {
	SID   segment.ID
	Pairs []KeyHash
}

type KeyHash struct {
	Key  string
	Hash [32]byte
}

// BuildSegKeys builds a SegKeys payload from a snapshot for the given SIDs.
func BuildSegKeys(view map[string]materialize.State, sids []segment.ID) SegKeys {
	want := make(map[segment.ID]struct{}, len(sids))
	for _, sid := range sids {
		want[sid] = struct{}{}
	}

	// Use materialized leaves to get (key, hash) pairs.
	leaves := materialize.LeavesFromSnapshot(view)

	per := map[segment.ID][]KeyHash{}
	for _, lf := range leaves {
		sid := segment.ForKey(lf.Key)
		if _, ok := want[sid]; !ok {
			continue
		}
		per[sid] = append(per[sid], KeyHash{Key: lf.Key, Hash: lf.Hash})
	}

	items := make([]SegKeysItem, 0, len(per))
	for sid, list := range per {
		sort.Slice(list, func(i, j int) bool { return list[i].Key < list[j].Key })
		items = append(items, SegKeysItem{SID: sid, Pairs: list})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].SID < items[j].SID })
	return SegKeys{Items: items}
}

// ----- codec (simple, fixed-endian like others) -----

func EncodeSegKeysReq(r SegKeysReq) []byte {
	var buf bytes.Buffer
	putU32(&buf, uint32(len(r.SIDs)))
	for _, sid := range r.SIDs {
		putU16(&buf, uint16(sid))
	}
	return buf.Bytes()
}

func DecodeSegKeysReq(b []byte) (SegKeysReq, error) {
	r := bytes.NewReader(b)
	n, err := getU32(r)
	if err != nil {
		return SegKeysReq{}, err
	}
	sids := make([]segment.ID, 0, n)
	for range int(n) {
		sid, err := getU16(r)
		if err != nil {
			return SegKeysReq{}, err
		}
		sids = append(sids, segment.ID(sid))
	}
	if r.Len() != 0 {
		return SegKeysReq{}, errors.New("trailing bytes in segkeys_req")
	}
	return SegKeysReq{SIDs: sids}, nil
}

func EncodeSegKeys(sk SegKeys) []byte {
	var buf bytes.Buffer
	putU32(&buf, uint32(len(sk.Items)))
	for _, it := range sk.Items {
		putU16(&buf, uint16(it.SID))
		putU32(&buf, uint32(len(it.Pairs)))
		for _, p := range it.Pairs {
			putU16(&buf, uint16(len(p.Key)))
			buf.WriteString(p.Key)
			buf.Write(p.Hash[:])
		}
	}
	return buf.Bytes()
}

func DecodeSegKeys(b []byte) (SegKeys, error) {
	r := bytes.NewReader(b)
	n, err := getU32(r)
	if err != nil {
		return SegKeys{}, err
	}
	items := make([]SegKeysItem, 0, n)
	for range int(n) {
		sidU16, err := getU16(r)
		if err != nil {
			return SegKeys{}, err
		}
		cnt, err := getU32(r)
		if err != nil {
			return SegKeys{}, err
		}
		pairs := make([]KeyHash, 0, cnt)
		for range int(cnt) {
			klen, err := getU16(r)
			if err != nil {
				return SegKeys{}, err
			}
			k := make([]byte, klen)
			if _, err := io.ReadFull(r, k); err != nil {
				return SegKeys{}, err
			}
			var h [32]byte
			if _, err := io.ReadFull(r, h[:]); err != nil {
				return SegKeys{}, err
			}
			pairs = append(pairs, KeyHash{Key: string(k), Hash: h})
		}
		items = append(items, SegKeysItem{SID: segment.ID(sidU16), Pairs: pairs})
	}
	if r.Len() != 0 {
		return SegKeys{}, errors.New("trailing bytes in segkeys")
	}
	return SegKeys{Items: items}, nil
}
