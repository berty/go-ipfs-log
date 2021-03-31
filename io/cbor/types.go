package cbor

import (
	"encoding/hex"

	"github.com/ipfs/go-cid"

	"berty.tech/go-ipfs-log/errmsg"
	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/iface"
)

// Entry CBOR representable version of Entry
type Entry struct {
	V        uint64
	LogID    string
	Key      string
	Sig      string
	Hash     interface{}
	Next     []cid.Cid
	Refs     []cid.Cid
	Clock    *LamportClock
	Payload  string
	Identity *Identity
}

// EntryV1 CBOR representable version of Entry v1
type EntryV1 struct {
	V        uint64
	LogID    string
	Key      string
	Sig      string
	Hash     interface{}
	Next     []cid.Cid
	Clock    *LamportClock
	Payload  string
	Identity *Identity
}

// EntryV2 CBOR representable version of Entry v2
type EntryV2 = Entry

// ToEntry returns a plain Entry from a CBOR serialized version
func (i *io) ToEntry(c *Entry, provider identityprovider.Interface) (iface.IPFSLogEntry, error) {
	key, err := hex.DecodeString(c.Key)
	if err != nil {
		return nil, errmsg.ErrKeyDeserialization.Wrap(err)
	}

	sig, err := hex.DecodeString(c.Sig)
	if err != nil {
		return nil, errmsg.ErrSigDeserialization.Wrap(err)
	}

	clock, err := i.ToLamportClock(c.Clock)
	if err != nil {
		return nil, errmsg.ErrClockDeserialization.Wrap(err)
	}

	identity, err := c.Identity.ToIdentity(provider)
	if err != nil {
		return nil, errmsg.ErrIdentityDeserialization.Wrap(err)
	}

	e := i.refEntry.New()
	e.SetV(c.V)
	e.SetLogID(c.LogID)
	e.SetKey(key)
	e.SetSig(sig)
	e.SetNext(c.Next)
	e.SetRefs(c.Refs)
	e.SetClock(clock)
	e.SetPayload([]byte(c.Payload))
	e.SetIdentity(identity)

	return e, nil
}

func (i *io) ToLamportClock(c *LamportClock) (iface.IPFSLogLamportClock, error) {
	id, err := hex.DecodeString(c.ID)
	if err != nil {
		return nil, errmsg.ErrClockDeserialization.Wrap(err)
	}

	n := i.refClock.New()
	n.SetID(id)
	n.SetTime(c.Time)

	return n, nil
}
