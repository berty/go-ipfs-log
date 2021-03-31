package cbor

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"

	"github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	format "github.com/ipfs/go-ipld-format"
	core_iface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/path"
	ic "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/polydawn/refmt/obj/atlas"

	"berty.tech/go-ipfs-log/errmsg"
	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/iface"
)

type io struct {
	debug bool

	refClock iface.IPFSLogLamportClock
	refEntry iface.IPFSLogEntry
}

func (i *io) DecodeRawLogHeads(node format.Node) (*iface.LogHeads, error) {
	logHeads := &iface.LogHeads{}
	err := cbornode.DecodeInto(node.RawData(), logHeads)

	if err != nil {
		return nil, errmsg.ErrCBOROperationFailed.Wrap(err)
	}

	return logHeads, nil
}

func (i *io) DecodeRawEntry(node format.Node, hash cid.Cid, p identityprovider.Interface) (iface.IPFSLogEntry, error) {
	obj := &Entry{}
	err := cbornode.DecodeInto(node.RawData(), obj)
	if err != nil {
		return nil, errmsg.ErrCBOROperationFailed.Wrap(err)
	}

	obj.Hash = hash

	e, err := i.ToEntry(obj, p)
	if err != nil {
		return nil, errmsg.ErrEntryDeserializationFailed.Wrap(err)
	}

	e.SetHash(hash)

	return e, nil
}

var _io = (*io)(nil)

func IO(refEntry iface.IPFSLogEntry, refClock iface.IPFSLogLamportClock) (iface.IO, error) {
	if _io != nil {
		return _io, nil
	}

	_io = &io{
		debug:    false,
		refClock: refClock,
		refEntry: refEntry,
	}

	cbornode.RegisterCborType(atlas.BuildEntry(Entry{}).
		StructMap().
		AddField("V", atlas.StructMapEntry{SerialName: "v"}).
		AddField("LogID", atlas.StructMapEntry{SerialName: "id"}).
		AddField("Key", atlas.StructMapEntry{SerialName: "key"}).
		AddField("Sig", atlas.StructMapEntry{SerialName: "sig"}).
		AddField("Hash", atlas.StructMapEntry{SerialName: "hash"}).
		AddField("Next", atlas.StructMapEntry{SerialName: "next"}).
		AddField("Refs", atlas.StructMapEntry{SerialName: "refs"}).
		AddField("Clock", atlas.StructMapEntry{SerialName: "clock"}).
		AddField("Payload", atlas.StructMapEntry{SerialName: "payload"}).
		AddField("Identity", atlas.StructMapEntry{SerialName: "identity"}).
		Complete())

	cbornode.RegisterCborType(atlas.BuildEntry(EntryV1{}).
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
		Complete())

	cbornode.RegisterCborType(atlas.BuildEntry(iface.Hashable{}).
		StructMap().
		AddField("Hash", atlas.StructMapEntry{SerialName: "hash"}).
		AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
		AddField("Payload", atlas.StructMapEntry{SerialName: "payload"}).
		AddField("Next", atlas.StructMapEntry{SerialName: "next"}).
		AddField("Refs", atlas.StructMapEntry{SerialName: "refs"}).
		AddField("V", atlas.StructMapEntry{SerialName: "v"}).
		AddField("Clock", atlas.StructMapEntry{SerialName: "clock"}).
		Complete())

	cbornode.RegisterCborType(atlas.BuildEntry(LamportClock{}).
		StructMap().
		AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
		AddField("Time", atlas.StructMapEntry{SerialName: "time"}).
		Complete())

	cbornode.RegisterCborType(atlas.BuildEntry(Identity{}).
		StructMap().
		AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
		AddField("Type", atlas.StructMapEntry{SerialName: "type"}).
		AddField("PublicKey", atlas.StructMapEntry{SerialName: "publicKey"}).
		AddField("Signatures", atlas.StructMapEntry{SerialName: "signatures"}).
		Complete())

	cbornode.RegisterCborType(atlas.BuildEntry(IdentitySignature{}).
		StructMap().
		AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
		AddField("PublicKey", atlas.StructMapEntry{SerialName: "publicKey"}).
		Complete())

	cbornode.RegisterCborType(atlas.BuildEntry(ic.Secp256k1PublicKey{}).
		Transform().
		TransformMarshal(atlas.MakeMarshalTransformFunc(
			func(x ic.Secp256k1PublicKey) (string, error) {
				keyBytes, err := x.Raw()
				if err != nil {
					return "", errmsg.ErrNotSecp256k1PubKey.Wrap(err)
				}

				return base64.StdEncoding.EncodeToString(keyBytes), nil
			})).
		TransformUnmarshal(atlas.MakeUnmarshalTransformFunc(
			func(x string) (ic.Secp256k1PublicKey, error) {
				keyBytes, err := base64.StdEncoding.DecodeString(x)
				if err != nil {
					return ic.Secp256k1PublicKey{}, errmsg.ErrNotSecp256k1PubKey.Wrap(err)
				}

				key, err := ic.UnmarshalSecp256k1PublicKey(keyBytes)
				if err != nil {
					return ic.Secp256k1PublicKey{}, errmsg.ErrNotSecp256k1PubKey.Wrap(err)
				}
				secpKey, ok := key.(*ic.Secp256k1PublicKey)
				if !ok {
					return ic.Secp256k1PublicKey{}, errmsg.ErrNotSecp256k1PubKey
				}

				return *secpKey, nil
			})).
		Complete())

	cbornode.RegisterCborType(atlas.BuildEntry(iface.LogHeads{}).
		StructMap().
		AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
		AddField("Heads", atlas.StructMapEntry{SerialName: "heads"}).
		Complete())

	return _io, nil
}

func (i *io) SetDebug(val bool) {
	i.debug = val
}

// WriteCBOR writes a CBOR representation of a given object in IPFS' DAG.
func (i *io) Write(ctx context.Context, ipfs core_iface.CoreAPI, obj interface{}, opts *iface.WriteOpts) (cid.Cid, error) {
	if opts == nil {
		opts = &iface.WriteOpts{}
	}

	switch o := obj.(type) {
	case iface.IPFSLogEntry:
		obj = i.ToCborEntry(o)
		break

	case *iface.LogHeads:
		break

	default:
		return cid.Undef, errmsg.ErrKeyNotDefined.Wrap(fmt.Errorf("unhandled type %T", obj))
	}

	cborNode, err := cbornode.WrapObject(obj, math.MaxUint64, -1)
	if err != nil {
		return cid.Undef, errmsg.ErrCBOROperationFailed.Wrap(err)
	}

	if i.debug {
		fmt.Printf("\nStr of cbor: %x\n", cborNode.RawData())
	}

	err = ipfs.Dag().Add(ctx, cborNode)
	if err != nil {
		return cid.Undef, errmsg.ErrIPFSOperationFailed.Wrap(err)
	}

	if opts.Pin {
		if err = ipfs.Pin().Add(ctx, path.IpfsPath(cborNode.Cid())); err != nil {
			return cid.Undef, errmsg.ErrIPFSOperationFailed.Wrap(err)
		}
	}

	return cborNode.Cid(), nil
}

// Read reads a CBOR representation of a given object from IPFS' DAG.
func (i *io) Read(ctx context.Context, ipfs core_iface.CoreAPI, contentIdentifier cid.Cid) (format.Node, error) {
	return ipfs.Dag().Get(ctx, contentIdentifier)
}

// ToCborEntry creates a CBOR serializable version of an entry
func (i *io) ToCborEntry(e iface.IPFSLogEntry) interface{} {
	if e.GetV() == 1 {
		return &EntryV1{
			V:        e.GetV(),
			LogID:    e.GetLogID(),
			Key:      hex.EncodeToString(e.GetKey()),
			Sig:      hex.EncodeToString(e.GetSig()),
			Hash:     nil,
			Next:     e.GetNext(),
			Clock:    i.ToCborLamportClock(e.GetClock()),
			Payload:  string(e.GetPayload()),
			Identity: i.ToCborIdentity(e.GetIdentity()),
		}
	}

	return &Entry{
		V:        e.GetV(),
		LogID:    e.GetLogID(),
		Key:      hex.EncodeToString(e.GetKey()),
		Sig:      hex.EncodeToString(e.GetSig()),
		Hash:     nil,
		Next:     e.GetNext(),
		Refs:     e.GetRefs(),
		Clock:    i.ToCborLamportClock(e.GetClock()),
		Payload:  string(e.GetPayload()),
		Identity: i.ToCborIdentity(e.GetIdentity()),
	}
}

type LamportClock struct {
	ID   string
	Time int
}

func (i *io) ToCborLamportClock(l iface.IPFSLogLamportClock) *LamportClock {
	return &LamportClock{
		ID:   hex.EncodeToString(l.GetID()),
		Time: l.GetTime(),
	}
}

// ToCborIdentity converts an identity to a CBOR serializable identity.
func (i *io) ToCborIdentity(id *identityprovider.Identity) *Identity {
	return &Identity{
		ID:         id.ID,
		PublicKey:  hex.EncodeToString(id.PublicKey),
		Type:       id.Type,
		Signatures: i.ToCborIdentitySignature(id.Signatures),
	}
}

// ToIdentity converts a CBOR serializable to a plain Identity object.
func (c *Identity) ToIdentity(provider identityprovider.Interface) (*identityprovider.Identity, error) {
	publicKey, err := hex.DecodeString(c.PublicKey)
	if err != nil {
		return nil, errmsg.ErrIdentityDeserialization.Wrap(err)
	}

	idSignatures, err := c.Signatures.ToIdentitySignature()
	if err != nil {
		return nil, errmsg.ErrIdentityDeserialization.Wrap(err)
	}

	return &identityprovider.Identity{
		Signatures: idSignatures,
		PublicKey:  publicKey,
		Type:       c.Type,
		ID:         c.ID,
		Provider:   provider,
	}, nil
}

// ToCborIdentitySignature converts to a CBOR serialized identity signature a plain IdentitySignature.
func (i *io) ToCborIdentitySignature(id *identityprovider.IdentitySignature) *IdentitySignature {
	return &IdentitySignature{
		ID:        hex.EncodeToString(id.ID),
		PublicKey: hex.EncodeToString(id.PublicKey),
	}
}

// ToIdentitySignature converts a CBOR serializable identity signature to a plain IdentitySignature.
func (c *IdentitySignature) ToIdentitySignature() (*identityprovider.IdentitySignature, error) {
	publicKey, err := hex.DecodeString(c.PublicKey)
	if err != nil {
		return nil, errmsg.ErrIdentitySigDeserialization.Wrap(err)
	}

	id, err := hex.DecodeString(c.ID)
	if err != nil {
		return nil, errmsg.ErrIdentitySigDeserialization.Wrap(err)
	}

	return &identityprovider.IdentitySignature{
		PublicKey: publicKey,
		ID:        id,
	}, nil
}

type IdentitySignature struct {
	ID        string
	PublicKey string
}

type Identity struct {
	ID         string
	PublicKey  string
	Signatures *IdentitySignature
	Type       string
}
