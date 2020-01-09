package entry

import (
	"encoding/hex"

	"berty.tech/go-ipfs-log/identity"
	"github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	"github.com/polydawn/refmt/obj/atlas"
)

// CborEntry CBOR representable version of Entry
type CborEntry struct {
	V        uint64
	LogID    string
	Key      string
	Sig      string
	Hash     interface{}
	Next     []cid.Cid
	Clock    *CborLamportClock
	Payload  string
	Identity *identity.CborIdentity
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

// ToEntry returns a plain Entry from a CBOR serialized version
func (c *CborEntry) ToEntry(provider identity.Provider) (*Entry, error) {
	key, err := hex.DecodeString(c.Key)
	if err != nil {
		return nil, err
	}

	sig, err := hex.DecodeString(c.Sig)
	if err != nil {
		return nil, err
	}

	clock, err := ToLamportClock(c.Clock)
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
