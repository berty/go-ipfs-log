package entry

import (
	"context"
	"encoding/hex"
	"math"

	"berty.tech/go-ipfs-log/identity"
	"berty.tech/go-ipfs-log/iface"
	"berty.tech/go-ipfs-log/io"
	"github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	"github.com/pkg/errors"
)

type Entry struct {
	Payload  []byte             `json:"payload,omitempty"`
	LogID    string             `json:"id,omitempty"`
	Next     []cid.Cid          `json:"next,omitempty"`
	V        uint64             `json:"v,omitempty"`
	Key      []byte             `json:"key,omitempty"`
	Sig      []byte             `json:"sig,omitempty"`
	Identity *identity.Identity `json:"identity,omitempty"`
	Hash     cid.Cid            `json:"hash,omitempty"`
	Clock    *LamportClock      `json:"clock,omitempty"`
}

// CreateEntry creates an Entry.
func CreateEntry(ctx context.Context, ipfsInstance io.IpfsServices, identity *identity.Identity, data *Entry, clock iface.IPFSLogLamportClock) (*Entry, error) {
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
	data.Clock = &LamportClock{
		ID:   clock.GetID(),
		Time: clock.GetTime(),
	}
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

	nd, err := cbornode.WrapObject(data.ToCborEntry(), math.MaxUint64, -1)
	if err != nil {
		return nil, err
	}

	err = ipfsInstance.Dag().Add(ctx, nd)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (e *Entry) GetPayload() []byte {
	return e.Payload
}

func (e *Entry) GetLogID() string {
	return e.LogID
}

func (e *Entry) GetNext() []cid.Cid {
	return e.Next
}

func (e *Entry) GetV() uint64 {
	return e.V
}

func (e *Entry) GetKey() []byte {
	return e.Key
}

func (e *Entry) GetSig() []byte {
	return e.Sig
}

func (e *Entry) GetIdentity() *identity.Identity {
	return e.Identity
}

func (e *Entry) GetHash() cid.Cid {
	return e.Hash
}

func (e *Entry) GetClock() iface.IPFSLogLamportClock {
	return e.Clock
}

func (e *Entry) SetPayload(payload []byte) {
	e.Payload = payload
}

func (e *Entry) SetLogID(logID string) {
	e.LogID = logID
}

func (e *Entry) SetNext(next []cid.Cid) {
	e.Next = next
}

func (e *Entry) SetV(v uint64) {
	e.V = v
}

func (e *Entry) SetKey(key []byte) {
	e.Key = key
}

func (e *Entry) SetSig(sig []byte) {
	e.Sig = sig
}

func (e *Entry) SetIdentity(identity *identity.Identity) {
	e.Identity = identity
}

func (e *Entry) SetHash(hash cid.Cid) {
	e.Hash = hash
}

func (e *Entry) SetClock(clock iface.IPFSLogLamportClock) {
	e.Clock = &LamportClock{
		ID:   clock.GetID(),
		Time: clock.GetTime(),
	}
}

// ToCborEntry creates a CBOR serializable version of an entry
func (e *Entry) ToCborEntry() interface{} {
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
func (e *Entry) IsValid() bool {
	return e.LogID != "" && len(e.Payload) > 0 && e.V <= 1
}

// Verify checks the entry's signature.
func (e *Entry) Verify(identity identity.Provider) error {
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

	pubKey, err := identity.UnmarshalPublicKey(e.Key)
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

	entryCID, err := io.WriteCBOR(ctx, ipfsInstance, data.ToCborEntry())

	return entryCID, err
}

// Equals checks that two entries are identical.
func (e *Entry) Equals(b *Entry) bool {
	return e.Hash.String() == b.Hash.String()
}

func (e *Entry) IsParent(b iface.IPFSLogEntry) bool {
	for _, next := range b.GetNext() {
		if next.String() == e.Hash.String() {
			return true
		}
	}

	return false
}

var _ iface.IPFSLogEntry = (*Entry)(nil)
