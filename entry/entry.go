// Package entry defines the Entry structure for IPFS Log and its associated methods.
package entry // import "berty.tech/go-ipfs-log/entry"

import (
	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/io"
	"context"
	"encoding/hex"
	"encoding/json"
	"github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	ic "github.com/libp2p/go-libp2p-crypto"
	"github.com/pkg/errors"
	"github.com/polydawn/refmt/obj/atlas"
	"math"
	"sort"
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
	Clock    *LamportClock
}

type Hashable struct {
	Hash    interface{}
	ID      string
	Payload []byte
	Next    []string
	V       uint64
	Clock   *LamportClock
	Key     []byte
}

var _ = atlas.BuildEntry(Hashable{}).
	StructMap().
	AddField("Hash", atlas.StructMapEntry{SerialName: "hash"}).
	AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
	AddField("Payload", atlas.StructMapEntry{SerialName: "payload"}).
	AddField("Next", atlas.StructMapEntry{SerialName: "next"}).
	AddField("V", atlas.StructMapEntry{SerialName: "v"}).
	AddField("Clock", atlas.StructMapEntry{SerialName: "clock"}).
	Complete()

type cborEntry struct {
	V        uint64
	LogID    string
	Key      string
	Sig      string
	Hash     interface{}
	Next     []cid.Cid
	Clock    *cborLamportClock
	Payload  string
	Identity *identityprovider.CborIdentity
}

func (c *cborEntry) toEntry(provider identityprovider.Interface) (*Entry, error) {
	key, err := hex.DecodeString(c.Key)
	if err != nil {
		return nil, err
	}

	sig, err := hex.DecodeString(c.Sig)
	if err != nil {
		return nil, err
	}

	clock, err := c.Clock.toLamportClock()
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

func (e *Entry) toCborEntry() *cborEntry {
	return &cborEntry{
		V:        e.V,
		LogID:    e.LogID,
		Key:      hex.EncodeToString(e.Key),
		Sig:      hex.EncodeToString(e.Sig),
		Hash:     nil,
		Next:     e.Next,
		Clock:    e.Clock.toCborLamportClock(),
		Payload:  string(e.Payload),
		Identity: e.Identity.ToCborIdentity(),
	}
}

func init() {
	AtlasEntry := atlas.BuildEntry(cborEntry{}).
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

// CreateEntry creates an Entry.
func CreateEntry(ctx context.Context, ipfsInstance io.IpfsServices, identity *identityprovider.Identity, data *Entry, clock *LamportClock) (*Entry, error) {
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
		return nil, errors.New("'LogID' is required")
	}

	if clock == nil {
		clock = NewLamportClock(identity.PublicKey, 0)
	}

	data = data.copy()
	data.Clock = clock
	data.V = 1

	jsonBytes, err := toBuffer(data.toHashable())
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
	data.Hash, err = data.ToMultihash(ctx, ipfsInstance)
	if err != nil {
		return nil, err
	}

	nd, err := cbornode.WrapObject(data.toCborEntry(), math.MaxUint64, -1)
	if err != nil {
		return nil, err
	}

	err = ipfsInstance.Dag().Add(ctx, nd)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// copy creates a copy of an entry.
func (e *Entry) copy() *Entry {
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

// uniqueCIDs returns uniques CIDs from a given list.
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

// toBuffer converts a hashable entry to bytes.
func toBuffer(e *Hashable) ([]byte, error) {
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

// toHashable Converts an entry to hashable.
func (e *Entry) toHashable() *Hashable {
	nexts := []string{}

	for _, n := range e.Next {
		nexts = append(nexts, n.String())
	}

	return &Hashable{
		Hash:    nil,
		ID:      e.LogID,
		Payload: e.Payload,
		Next:    nexts,
		V:       e.V,
		Clock:   e.Clock,
		Key:     e.Key,
	}
}

// isValid checks that an entry is valid.
func (e *Entry) isValid() bool {
	return e.LogID != "" && len(e.Payload) > 0 && e.V <= 1
}

// Verify checks the entry's signature.
func (e *Entry) Verify(identity identityprovider.Interface) error {
	if e == nil {
		return errors.New("entry is not defined")
	}

	if len(e.Key) == 0 {
		return errors.New("entry doesn't have a key")
	}

	if len(e.Sig) == 0 {
		return errors.New("entry doesn't have a signature")
	}

	// TODO: Check against trusted keys

	jsonBytes, err := toBuffer(e.toHashable())
	if err != nil {
		return errors.Wrap(err, "unable to build string buffer")
	}

	pubKey, err := ic.UnmarshalSecp256k1PublicKey(e.Key)
	if err != nil {
		return errors.Wrap(err, "unable to unmarshal public key")
	}

	ok, err := pubKey.Verify(jsonBytes, e.Sig)
	if err != nil {
		return errors.Wrap(err, "error whild verifying signature")
	}

	if !ok {
		return errors.New("unable to verify entry signature")
	}

	return nil
}

// ToMultihash gets the multihash of an Entry.
func (e *Entry) ToMultihash(ctx context.Context, ipfsInstance io.IpfsServices) (cid.Cid, error) {
	if e == nil {
		return cid.Cid{}, errors.New("entry is not defined")
	}

	if ipfsInstance == nil {
		return cid.Cid{}, errors.New("ipfs instance not defined")
	}

	data := &Entry{
		Hash:    cid.Cid{},
		LogID:   e.LogID,
		Payload: e.Payload,
		Next:    e.Next,
		V:       e.V,
		Clock:   e.Clock,
	}

	if e.Key != nil {
		data.Key = e.Key
	}

	if e.Identity != nil {
		data.Identity = e.Identity
	}

	if len(e.Sig) > 0 {
		data.Sig = e.Sig
	}

	entryCID, err := io.WriteCBOR(ctx, ipfsInstance, data.toCborEntry())

	return entryCID, err
}

// fromMultihash creates an Entry from a hash.
func fromMultihash(ctx context.Context, ipfs io.IpfsServices, hash cid.Cid, provider identityprovider.Interface) (*Entry, error) {
	if ipfs == nil {
		return nil, errors.New("ipfs instance not defined")
	}

	result, err := io.ReadCBOR(ctx, ipfs, hash)
	if err != nil {
		return nil, err
	}

	obj := &cborEntry{}
	err = cbornode.DecodeInto(result.RawData(), obj)
	if err != nil {
		return nil, err
	}

	obj.Hash = hash

	entry, err := obj.toEntry(provider)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

// Equals checks that two entries are identical.
func (e *Entry) Equals(b *Entry) bool {
	return e.Hash.String() == b.Hash.String()
}

func (e *Entry) IsParent(b *Entry) bool {
	for _, next := range b.Next {
		if next.String() == e.Hash.String() {
			return true
		}
	}

	return false
}

// FindChildren finds an entry's children from an Array of entries.
//
// Returns entry's children as an Array up to the last know child.
func FindChildren(entry *Entry, values []*Entry) []*Entry {
	stack := []*Entry{}

	var parent *Entry
	for _, e := range values {
		if entry.IsParent(e) {
			parent = e
			break
		}
	}

	for parent != nil {
		stack = append(stack, parent)
		prev := parent

		for _, e := range values {
			if prev.IsParent(e) {
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
