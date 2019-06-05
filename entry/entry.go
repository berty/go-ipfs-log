package entry

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"math"
	"sort"
	"time"

	"github.com/berty/go-ipfs-log/identityprovider"
	"github.com/berty/go-ipfs-log/io"
	"github.com/berty/go-ipfs-log/utils/lamportclock"
	"github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	ic "github.com/libp2p/go-libp2p-crypto"
	"github.com/pkg/errors"
	_ "github.com/polydawn/refmt"
	"github.com/polydawn/refmt/obj/atlas"
)

type Entry struct {
	Payload  []byte
	LogID    string
	Next     []cid.Cid
	V        uint64
	Key      []byte
	Sig      []byte
	Identity *identityprovider.Identity
	Hash     cid.Cid
	Clock    *lamportclock.LamportClock
}

type EntryToHash struct {
	Hash    interface{}
	ID      string
	Payload []byte
	Next    []string
	V       uint64
	Clock   *lamportclock.LamportClock
	Key     []byte
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

type CborEntry struct {
	V        uint64
	LogID    string
	Key      string
	Sig      string
	Hash     interface{}
	Next     []cid.Cid
	Clock    *lamportclock.CborLamportClock
	Payload  string
	Identity *identityprovider.CborIdentity
}

func (c *CborEntry) ToEntry(provider identityprovider.Interface) (*Entry, error) {
	key, err := hex.DecodeString(c.Key)
	if err != nil {
		return nil, err
	}

	sig, err := hex.DecodeString(c.Sig)
	if err != nil {
		return nil, err
	}

	clock, err := c.Clock.ToLamportClock()
	if err != nil {
		return nil, err
	}

	identity, err := c.Identity.ToIdentity(provider)
	if err != nil {
		return nil, err
	}

	return &Entry{
		V:        c.V,
		LogID:    c.LogID,
		Key:      key,
		Sig:      sig,
		Next:     c.Next,
		Clock:    clock,
		Payload:  []byte(c.Payload),
		Identity: identity,
	}, nil
}

func (e *Entry) ToCborEntry() *CborEntry {
	return &CborEntry{
		V:        e.V,
		LogID:    e.LogID,
		Key:      hex.EncodeToString(e.Key),
		Sig:      hex.EncodeToString(e.Sig),
		Hash:     nil,
		Next:     e.Next,
		Clock:    e.Clock.ToCborLamportClock(),
		Payload:  string(e.Payload),
		Identity: e.Identity.ToCborIdentity(),
	}
}

func init() {
	AtlasEntry := atlas.BuildEntry(CborEntry{}).
		StructMap().
		AddField("V", atlas.StructMapEntry{SerialName: "v"}).
		AddField("LogID", atlas.StructMapEntry{SerialName: "id"}).
		AddField("Key", atlas.StructMapEntry{SerialName: "key"}).
		AddField("Sig", atlas.StructMapEntry{SerialName: "sig"}).
		AddField("Hash", atlas.StructMapEntry{SerialName: "hash"}).
		AddField("Next", atlas.StructMapEntry{SerialName: "next"}).
		AddField("Clock", atlas.StructMapEntry{SerialName: "clock"}).
		AddField("Payload", atlas.StructMapEntry{SerialName: "payload"}).
		AddField("Identity", atlas.StructMapEntry{SerialName: "identity"}).
		Complete()

	cbornode.RegisterCborType(AtlasEntry)
}

func CreateEntry(ipfsInstance *io.IpfsServices, identity *identityprovider.Identity, data *Entry, clock *lamportclock.LamportClock) (*Entry, error) {
	if ipfsInstance == nil {
		return nil, errors.New("ipfs instance not defined")
	}

	if identity == nil {
		return nil, errors.New("identity is required")
	}

	if data == nil {
		return nil, errors.New("data is not defined")
	}

	if data.LogID == "" {
		return nil, errors.New("LogID is required")
	}

	if clock == nil {
		clock = lamportclock.New(identity.PublicKey, 0)
	}

	data = data.Copy()
	data.Clock = clock
	data.V = 1

	jsonBytes, err := ToBuffer(data.ToHashable())
	if err != nil {
		return nil, err
	}

	signature, err := identity.Provider.Sign(identity, jsonBytes)

	if err != nil {
		return nil, err
	}

	data.Key = identity.PublicKey
	data.Sig = signature

	data.Identity = identity.Filtered()
	data.Hash, err = ToMultihash(ipfsInstance, data)
	if err != nil {
		return nil, err
	}

	nd, err := cbornode.WrapObject(data.ToCborEntry(), math.MaxUint64, -1)
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
		Payload:  e.Payload,
		LogID:    e.LogID,
		Next:     uniqueCIDs(e.Next),
		V:        e.V,
		Key:      e.Key,
		Sig:      e.Sig,
		Identity: e.Identity,
		Hash:     e.Hash,
		Clock:    e.Clock,
	}
}

func uniqueCIDs(cids []cid.Cid) []cid.Cid {
	foundCids := map[string]bool{}
	out := []cid.Cid{}

	for _, c := range cids {
		if _, ok := foundCids[c.String()]; ok {
			continue
		}

		foundCids[c.String()] = true
		out = append(out, c)
	}

	return out
}

func ToBuffer(e *EntryToHash) ([]byte, error) {
	if e == nil {
		return nil, errors.New("entry is not defined")
	}

	jsonBytes, err := json.Marshal(map[string]interface{}{
		"hash":    nil,
		"id":      e.ID,
		"payload": string(e.Payload),
		"next":    e.Next,
		"v":       e.V,
		"clock": map[string]interface{}{
			"id":   hex.EncodeToString(e.Clock.ID),
			"time": e.Clock.Time,
		},
	})
	if err != nil {
		return nil, err
	}

	return jsonBytes, nil
}

func (e *Entry) ToHashable() *EntryToHash {
	nexts := []string{}

	for _, n := range e.Next {
		nexts = append(nexts, n.String())
	}

	return &EntryToHash{
		Hash:    nil,
		ID:      e.LogID,
		Payload: e.Payload,
		Next:    nexts,
		V:       e.V,
		Clock:   e.Clock,
		Key:     e.Key,
	}
}

func (e *Entry) IsValid() bool {
	return e.LogID != "" && len(e.Payload) > 0 && e.V >= 0 && e.V <= 1
}

func Verify(identity identityprovider.Interface, entry *Entry) error {
	if entry == nil {
		return errors.New("entry is not defined")
	}

	if len(entry.Key) == 0 {
		return errors.New("Entry doesn't have a key")
	}

	if len(entry.Sig) == 0 {
		return errors.New("Entry doesn't have a signature")
	}

	// TODO: Check against trusted keys

	jsonBytes, err := ToBuffer(entry.ToHashable())
	if err != nil {
		return errors.Wrap(err, "unable to build string buffer")
	}

	pubKey, err := ic.UnmarshalSecp256k1PublicKey(entry.Key)
	if err != nil {
		return errors.Wrap(err, "unable to unmarshal public key")
	}

	ok, err := pubKey.Verify(jsonBytes, entry.Sig)
	if err != nil {
		return errors.Wrap(err, "error whild verifying signature")
	}

	if !ok {
		return errors.New("unable to verify entry signature")
	}

	return nil
}

func ToMultihash(ipfsInstance *io.IpfsServices, entry *Entry) (cid.Cid, error) {
	if entry == nil {
		return cid.Cid{}, errors.New("entry is not defined")
	}

	if ipfsInstance == nil {
		return cid.Cid{}, errors.New("ipfs instance not defined")
	}

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

	entryCID, err := io.WriteCBOR(ipfsInstance, e.ToCborEntry())

	return entryCID, err
}

func FromMultihash(ipfs *io.IpfsServices, hash cid.Cid, provider identityprovider.Interface) (*Entry, error) {
	if ipfs == nil {
		return nil, errors.New("ipfs instance not defined")
	}

	result, err := io.ReadCBOR(ipfs, hash)
	if err != nil {
		return nil, err
	}

	obj := &CborEntry{}
	err = cbornode.DecodeInto(result.RawData(), obj)
	if err != nil {
		return nil, err
	}

	obj.Hash = hash

	entry, err := obj.ToEntry(provider)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

func SortEntries(entries []*Entry) {
	sort.SliceStable(entries, func(i, j int) bool {
		ret, err := Compare(entries[i], entries[j])
		if err != nil {
			return false
		}
		return ret < 0
	})
}

func Compare(a, b *Entry) (int, error) {
	// TODO: Make it a Golang slice-compatible sort function
	if a == nil || b == nil {
		return 0, errors.New("entry is not defined")
	}

	return lamportclock.Compare(a.Clock, b.Clock), nil
}

func IsEqual(a, b *Entry) bool {
	return a.Hash.String() == b.Hash.String()
}

func IsParent(entry1, entry2 *Entry) bool {
	for _, next := range entry2.Next {
		if next.String() == entry1.Hash.String() {
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

			parent = nil
		}
	}

	sort.SliceStable(stack, func(i, j int) bool {
		return stack[i].Clock.Time <= stack[j].Clock.Time
	})

	return stack
}
