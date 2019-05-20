package entry

import (
	"bytes"
	"context"
	"github.com/berty/go-ipfs-log/identityprovider"
	"github.com/berty/go-ipfs-log/io"
	"github.com/berty/go-ipfs-log/utils/lamportclock"
	"github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	"github.com/ipfs/go-ipld-cbor/encoding"
	ic "github.com/libp2p/go-libp2p-crypto"
	mh "github.com/multiformats/go-multihash"
	"github.com/pkg/errors"
	_ "github.com/polydawn/refmt"
	"github.com/polydawn/refmt/obj/atlas"
	"math"
	"sort"
	"time"
)

type Entry struct {
	Payload  []byte
	LogID    string
	Next     []cid.Cid
	V        uint64
	Key      ic.PubKey
	Sig      mh.Multihash
	Identity *identityprovider.Identity
	Hash     cid.Cid
	Clock    lamportclock.LamportClock
}

type EntryToHash struct {
	Hash    interface{}
	ID      string
	Payload []byte
	Next    []cid.Cid
	V       uint64
	Clock   lamportclock.LamportClock
}

var AtlasEntryToHash = atlas.BuildEntry(EntryToHash{}).
	StructMap().
	AddField("Hash", atlas.StructMapEntry{SerialName: "hash"}).
	AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
	AddField("Payload", atlas.StructMapEntry{SerialName: "payload"}).
	AddField("Next", atlas.StructMapEntry{SerialName: "next"}).
	AddField("V", atlas.StructMapEntry{SerialName: "v"}).
	AddField("Clock", atlas.StructMapEntry{SerialName: "clock"}).
	Complete()

var AtlasLamportClock = atlas.BuildEntry(lamportclock.LamportClock{}).
	StructMap().
	AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
	AddField("Time", atlas.StructMapEntry{SerialName: "time"}).
	Complete()

func init() {
	AtlasEntry := atlas.BuildEntry(Entry{}).
		StructMap().
		AddField("Clock", atlas.StructMapEntry{SerialName: "clock"}).
		AddField("Identity", atlas.StructMapEntry{SerialName: "identity"}).
		AddField("Key", atlas.StructMapEntry{SerialName: "key"}).
		AddField("LogID", atlas.StructMapEntry{SerialName: "id"}).
		AddField("Next", atlas.StructMapEntry{SerialName: "next"}).
		AddField("Payload", atlas.StructMapEntry{SerialName: "payload"}).
		AddField("Sig", atlas.StructMapEntry{SerialName: "sig"}).
		Complete()

	cbornode.RegisterCborType(AtlasEntry)
	cbornode.RegisterCborType(AtlasLamportClock)
}

func CreateEntry(ipfsInstance *io.IpfsServices, identity *identityprovider.Identity, data *Entry, clock *lamportclock.LamportClock) (*Entry, error) {
	if ipfsInstance == nil {
		return nil, errors.New("ipfs services must be provided")
	}

	if clock == nil {
		clock = lamportclock.New(identity.PublicKey, 0)
	}

	signature, err := identity.PrivateKey.Sign(data.Payload)
	if err != nil {
		return nil, err
	}

	data = data.Copy()
	data.Key = identity.PublicKey
	data.Sig = signature
	data.V = 1
	data.Identity = identity.Filtered()
	data.Hash, err = io.WriteCBOR(ipfsInstance, data)
	if err != nil {
		return nil, err
	}

	nd, err := cbornode.WrapObject(data, math.MaxUint64, -1)
	if err != nil {
		return nil, err
	}

	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
	err = ipfsInstance.DAG.Add(ctx, nd)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (e *Entry) Copy() *Entry {
	return &Entry{
		Payload: e.Payload,
		LogID:   e.LogID,
		Next:    e.Next,

		Key:      e.Key,
		Sig:      e.Sig,
		Identity: e.Identity,
		Hash:     e.Hash,
		Clock:    e.Clock,
	}
}

func ToBuffer(e *EntryToHash) ([]byte, error) {
	atl, err := atlas.Build(AtlasEntryToHash, AtlasLamportClock)
	if err != nil {
		return nil, err
	}

	marshaller := encoding.NewMarshallerAtlased(atl)
	jsonBytes, err := marshaller.Marshal(e)
	if err != nil {
		return nil, err
	}

	return jsonBytes, nil
}

func (e *Entry) ToHashable() *EntryToHash {
	return &EntryToHash{
		Hash:    nil,
		ID:      e.LogID,
		Payload: e.Payload,
		Next:    e.Next,
		V:       e.V,
		Clock:   e.Clock,
	}
}

func (e *Entry) IsValid() bool {
	return e.LogID != "" && len(e.Payload) > 0 && e.V >= 0 && e.V <= 1
}

func Verify(identity identityprovider.Interface, entry *Entry) error {
	//
	jsonBytes, err := ToBuffer(entry.ToHashable())

	ok, err := entry.Key.Verify(jsonBytes, entry.Sig)
	if err != nil {
		return err
	}

	if !ok {
		return errors.New("unable to verify entry signature")
	}

	return nil
}

func ToMultihash(ipfsInstance *io.IpfsServices, entry *Entry) (cid.Cid, error) {
	e := &Entry{
		Hash:    cid.Cid{},
		LogID:   entry.LogID,
		Payload: entry.Payload,
		Next:    entry.Next,
		V:       entry.V,
		Clock:   entry.Clock,
	}

	if entry.Key != nil {
		e.Key = entry.Key
	}

	if entry.Identity != nil {
		e.Identity = entry.Identity
	}

	if len(entry.Sig) > 0 {
		e.Sig = entry.Sig
	}

	entryCID, err := io.WriteCBOR(ipfsInstance, e)

	return entryCID, err
}

func FromMultihash(ipfs *io.IpfsServices, hash cid.Cid) (*Entry, error) {
	result, err := io.ReadCBOR(ipfs, hash)
	if err != nil {
		return nil, err
	}

	obj := &Entry{}
	err = cbornode.DecodeInto(result.RawData(), obj)
	if err != nil {
		return nil, err
	}

	return obj, nil
}

func Compare(a, b *Entry) (int, error) {
	// TODO: Make it a Golang slice-compatible sort function

	distance, err := lamportclock.Compare(&a.Clock, &b.Clock)
	if err != nil {
		return 0, err
	}

	if distance == 0 {
		aClockBytes, err := a.Clock.ID.Bytes()
		if err != nil {
			return 0, err
		}

		bClockBytes, err := b.Clock.ID.Bytes()
		if err != nil {
			return 0, err
		}

		if bytes.Compare(aClockBytes, bClockBytes) < 0 {
			return -1, nil
		} else {
			return 1, nil
		}
	}

	return distance, nil
}

func IsEqual(a, b *Entry) bool {
	return a.Hash.String() == b.Hash.String()
}

func IsParent(entry1, entry2 *Entry) bool {
	for _, next := range entry2.Next {
		if next == entry2.Hash {
			return true
		}
	}

	return false
}

func FindChildren(entry *Entry, values []*Entry) []*Entry {
	stack := []*Entry{}

	var parent *Entry
	for _, e := range values {
		if IsParent(entry, e) {
			parent = e
			break
		}
	}

	prev := entry
	for parent != nil {
		stack = append(stack, parent)
		prev = parent

		for _, e := range values {
			if IsParent(prev, e) {
				parent = e
				break
			}
		}
	}

	sort.SliceStable(stack, func(i, j int) bool {
		return stack[i].Clock.Time < stack[j].Clock.Time
	})

	return stack
}
