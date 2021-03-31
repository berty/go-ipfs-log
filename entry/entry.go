// Package entry defines the Entry structure for IPFS Log and its associated methods.
package entry // import "berty.tech/go-ipfs-log/entry"

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/ipfs/go-cid"
	core_iface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/multiformats/go-multibase"

	"berty.tech/go-ipfs-log/errmsg"
	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/iface"
	"berty.tech/go-ipfs-log/io/cbor"
)

type Entry struct {
	Payload  []byte                     `json:"payload,omitempty"`
	LogID    string                     `json:"id,omitempty"`
	Next     []cid.Cid                  `json:"next,omitempty"`
	Refs     []cid.Cid                  `json:"refs,omitempty"`
	V        uint64                     `json:"v,omitempty"`
	Key      []byte                     `json:"key,omitempty"`
	Sig      []byte                     `json:"sig,omitempty"`
	Identity *identityprovider.Identity `json:"identity,omitempty"`
	Hash     cid.Cid                    `json:"hash,omitempty"`
	Clock    iface.IPFSLogLamportClock  `json:"clock,omitempty"`
}

func (e *Entry) New() iface.IPFSLogEntry {
	return &Entry{}
}

func (e *Entry) GetRefs() []cid.Cid {
	return e.Refs
}

func (e *Entry) SetRefs(refs []cid.Cid) {
	e.Refs = refs
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

func (e *Entry) GetIdentity() *identityprovider.Identity {
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

func (e *Entry) SetIdentity(identity *identityprovider.Identity) {
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

func CreateEntry(ctx context.Context, ipfsInstance core_iface.CoreAPI, identity *identityprovider.Identity, data *Entry, opts *iface.CreateEntryOptions) (*Entry, error) {
	io, err := cbor.IO(&Entry{}, &LamportClock{})
	if err != nil {
		return nil, err
	}

	return CreateEntryWithIO(ctx, ipfsInstance, identity, data, opts, io)
}

// CreateEntryWithIO creates an Entry.
func CreateEntryWithIO(ctx context.Context, ipfsInstance core_iface.CoreAPI, identity *identityprovider.Identity, data *Entry, opts *iface.CreateEntryOptions, io iface.IO) (*Entry, error) {
	if ipfsInstance == nil {
		return nil, errmsg.ErrIPFSNotDefined
	}

	if identity == nil {
		return nil, errmsg.ErrIdentityNotDefined
	}

	if data == nil {
		return nil, errmsg.ErrPayloadNotDefined
	}

	if data.LogID == "" {
		return nil, errmsg.ErrLogIDNotDefined
	}

	data = data.copy()

	if data.Clock != nil {
		data.Clock = CopyLamportClock(data.GetClock())
	} else {
		data.Clock = NewLamportClock(identity.PublicKey, 0)
	}
	data.V = 2

	hashable, err := data.toHashable()
	if err != nil {
		return nil, errmsg.ErrEntryNotHashable.Wrap(err)
	}

	jsonBytes, err := toBuffer(hashable)
	if err != nil {
		return nil, errmsg.ErrEntryNotHashable.Wrap(err)
	}

	signature, err := identity.Provider.Sign(identity, jsonBytes)

	if err != nil {
		return nil, errmsg.ErrSigSign.Wrap(err)
	}

	data.Key = identity.PublicKey
	data.Sig = signature

	data.Identity = identity.Filtered()
	data.Hash, err = data.ToMultihashWithIO(ctx, ipfsInstance, opts, io)
	if err != nil {
		return nil, errmsg.ErrIPFSOperationFailed.Wrap(err)
	}

	return data, nil
}

// copy creates a copy of an entry.
func (e *Entry) copy() *Entry {
	return &Entry{
		Payload:  e.Payload,
		LogID:    e.LogID,
		Next:     uniqueCIDs(e.Next),
		Refs:     uniqueCIDs(e.Refs),
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

func cidB58(c cid.Cid) (string, error) {
	e, err := multibase.NewEncoder(multibase.Base58BTC)
	if err != nil {
		return "", errmsg.ErrMultibaseOperationFailed.Wrap(err)
	}

	return c.Encode(e), nil
}

// toBuffer converts a hashable entry to bytes.
func toBuffer(e *iface.Hashable) ([]byte, error) {
	if e == nil {
		return nil, errmsg.ErrEntryNotDefined
	}

	jsonBytes, err := json.Marshal(map[string]interface{}{
		"hash":    nil,
		"id":      e.ID,
		"payload": string(e.Payload),
		"next":    e.Next,
		"refs":    e.Refs,
		"v":       e.V,
		"clock": map[string]interface{}{
			"id":   hex.EncodeToString(e.Clock.GetID()),
			"time": e.Clock.GetTime(),
		},
	})
	if err != nil {
		return nil, errmsg.ErrJSONSerializationFailed.Wrap(err)
	}

	return jsonBytes, nil
}

// toHashable Converts an entry to hashable.
func (e *Entry) toHashable() (*iface.Hashable, error) {
	nexts := make([]string, len(e.Next))
	refs := make([]string, len(e.Refs))

	for i, n := range e.Next {
		c, err := cidB58(n)
		if err != nil {
			return nil, errmsg.ErrCIDSerializationFailed.Wrap(err)
		}

		nexts[i] = c
	}

	for i, r := range e.Refs {
		c, err := cidB58(r)
		if err != nil {
			return nil, errmsg.ErrCIDSerializationFailed.Wrap(err)
		}

		refs[i] = c
	}

	return &iface.Hashable{
		Hash:    nil,
		ID:      e.LogID,
		Payload: e.Payload,
		Next:    nexts,
		Refs:    refs,
		V:       e.V,
		Clock:   e.Clock,
		Key:     e.Key,
	}, nil
}

// isValid checks that an entry is valid.
func (e *Entry) IsValid() bool {
	ok := e.LogID != "" && len(e.Payload) > 0 && e.V <= 2

	return ok
}

// Verify checks the entry's signature.
func (e *Entry) Verify(identity identityprovider.Interface) error {
	if e == nil {
		return errmsg.ErrEntryNotDefined
	}

	if len(e.Key) == 0 {
		return errmsg.ErrKeyNotDefined
	}

	if len(e.Sig) == 0 {
		return errmsg.ErrSigNotDefined
	}

	// TODO: Check against trusted keys

	hashable, err := e.toHashable()
	if err != nil {
		return errmsg.ErrEntryNotHashable.Wrap(err)
	}

	jsonBytes, err := toBuffer(hashable)
	if err != nil {
		return errmsg.ErrEntryNotHashable.Wrap(err)
	}

	pubKey, err := identity.UnmarshalPublicKey(e.Key)
	if err != nil {
		return errmsg.ErrInvalidPubKeyFormat.Wrap(err)
	}

	ok, err := pubKey.Verify(jsonBytes, e.Sig)
	if err != nil {
		return errmsg.ErrSigNotVerified.Wrap(err)
	}

	if !ok {
		return errmsg.ErrSigNotVerified
	}

	return nil
}

// ToMultihash gets the multihash of an Entry.
func (e *Entry) ToMultihash(ctx context.Context, ipfsInstance core_iface.CoreAPI, opts *iface.CreateEntryOptions) (cid.Cid, error) {
	io, err := cbor.IO(&Entry{}, &LamportClock{})
	if err != nil {
		return cid.Undef, err
	}

	return e.ToMultihashWithIO(ctx, ipfsInstance, opts, io)
}

// ToMultihashWithIO gets the multihash of an Entry.
func (e *Entry) ToMultihashWithIO(ctx context.Context, ipfsInstance core_iface.CoreAPI, opts *iface.CreateEntryOptions, io iface.IO) (cid.Cid, error) {

	if opts == nil {
		opts = &iface.CreateEntryOptions{}
	}

	if e == nil {
		return cid.Undef, errmsg.ErrEntryNotDefined
	}

	if ipfsInstance == nil {
		return cid.Undef, errmsg.ErrIPFSNotDefined
	}

	data := e.copyNormalizedEntry(&normalizeEntryOpts{
		preSigned: opts.PreSigned,
	})

	return io.Write(ctx, ipfsInstance, data, &iface.WriteOpts{
		Pin: opts.Pin,
	})
}

type normalizeEntryOpts struct {
	preSigned   bool
	includeHash bool
}

func (e *Entry) copyNormalizedEntry(opts *normalizeEntryOpts) *Entry {
	if opts == nil {
		opts = &normalizeEntryOpts{}
	}

	data := &Entry{
		LogID:   e.LogID,
		Payload: e.Payload,
		Next:    e.Next,
		V:       e.V,
		Clock:   CopyLamportClock(e.GetClock()),
	}

	if opts.includeHash {
		data.Hash = e.Hash
	}

	if e.V > 1 {
		data.Refs = e.Refs
	}

	data.Key = e.Key
	data.Identity = e.Identity

	if opts.preSigned {
		return data
	}

	if len(e.Sig) > 0 {
		data.Sig = e.Sig
	}

	return data
}

// FromMultihash creates an Entry from a hash.
func FromMultihash(ctx context.Context, ipfs core_iface.CoreAPI, hash cid.Cid, provider identityprovider.Interface) (iface.IPFSLogEntry, error) {
	io, err := cbor.IO(&Entry{}, &LamportClock{})
	if err != nil {
		return nil, err
	}

	return FromMultihashWithIO(ctx, ipfs, hash, provider, io)
}

// FromMultihashWithIO creates an Entry from a hash.
func FromMultihashWithIO(ctx context.Context, ipfs core_iface.CoreAPI, hash cid.Cid, provider identityprovider.Interface, io iface.IO) (iface.IPFSLogEntry, error) {
	if ipfs == nil {
		return nil, errmsg.ErrIPFSNotDefined
	}

	result, err := io.Read(ctx, ipfs, hash)
	if err != nil {
		return nil, errmsg.ErrCBOROperationFailed.Wrap(err)
	}

	return io.DecodeRawEntry(result, hash, provider)
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

// FindChildren finds an entry's children from an Array of entries.
//
// Returns entry's children as an Array up to the last know child.
func FindChildren(entry iface.IPFSLogEntry, values []iface.IPFSLogEntry) []iface.IPFSLogEntry {
	var stack []iface.IPFSLogEntry

	var parent iface.IPFSLogEntry
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
		return stack[i].GetClock().GetTime() <= stack[j].GetClock().GetTime()
	})

	return stack
}

var _ iface.IPFSLogEntry = (*Entry)(nil)
