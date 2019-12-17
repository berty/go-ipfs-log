// Package identityprovider defines a default identity provider for IPFS Log and OrbitDB.
package identityprovider // import "berty.tech/go-ipfs-log/identityprovider"

import (
	"encoding/base64"
	"encoding/hex"

	cbornode "github.com/ipfs/go-ipld-cbor"
	ic "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/pkg/errors"
	"github.com/polydawn/refmt/obj/atlas"
)

type IdentitySignature struct {
	ID        []byte `json:"id,omitempty"`
	PublicKey []byte `json:"publicKey,omitempty"`
}

type CborIdentitySignature struct {
	ID        string
	PublicKey string
}

type Identity struct {
	ID         string             `json:"id,omitempty"`
	PublicKey  []byte             `json:"publicKey,omitempty"`
	Signatures *IdentitySignature `json:"signatures,omitempty"`
	Type       string             `json:"type,omitempty"`
	Provider   Interface
}

type CborIdentity struct {
	ID         string
	PublicKey  string
	Signatures *CborIdentitySignature
	Type       string
}

// Filtered gets fields that should be present in the CBOR representation of identity.
func (i *Identity) Filtered() *Identity {
	return &Identity{
		ID:         i.ID,
		PublicKey:  i.PublicKey,
		Signatures: i.Signatures,
		Type:       i.Type,
	}
}

// GetPublicKey returns the public key of an identity.
func (i *Identity) GetPublicKey() (ic.PubKey, error) {
	return ic.UnmarshalPublicKey(i.PublicKey)
}

var atlasIdentity = atlas.BuildEntry(CborIdentity{}).
	StructMap().
	AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
	AddField("Type", atlas.StructMapEntry{SerialName: "type"}).
	AddField("PublicKey", atlas.StructMapEntry{SerialName: "publicKey"}).
	AddField("Signatures", atlas.StructMapEntry{SerialName: "signatures"}).
	Complete()

var atlasIdentitySignature = atlas.BuildEntry(CborIdentitySignature{}).
	StructMap().
	AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
	AddField("PublicKey", atlas.StructMapEntry{SerialName: "publicKey"}).
	Complete()

var atlasPubKey = atlas.BuildEntry(ic.Secp256k1PublicKey{}).
	Transform().
	TransformMarshal(atlas.MakeMarshalTransformFunc(
		func(x ic.Secp256k1PublicKey) (string, error) {
			keyBytes, err := x.Raw()
			if err != nil {
				return "", err
			}

			return base64.StdEncoding.EncodeToString(keyBytes), nil
		})).
	TransformUnmarshal(atlas.MakeUnmarshalTransformFunc(
		func(x string) (ic.Secp256k1PublicKey, error) {
			keyBytes, err := base64.StdEncoding.DecodeString(x)
			if err != nil {
				return ic.Secp256k1PublicKey{}, err
			}

			key, err := ic.UnmarshalSecp256k1PublicKey(keyBytes)
			if err != nil {
				return ic.Secp256k1PublicKey{}, errors.Wrap(err, "failed to unmarshal public key")
			}
			secpKey, ok := key.(*ic.Secp256k1PublicKey)
			if !ok {
				return ic.Secp256k1PublicKey{}, errors.New("invalid public key")
			}

			return *secpKey, nil
		})).
	Complete()

func init() {
	cbornode.RegisterCborType(atlasIdentity)
	cbornode.RegisterCborType(atlasIdentitySignature)
	cbornode.RegisterCborType(atlasPubKey)
}

// ToCborIdentity converts an identity to a CBOR serializable identity.
func (i *Identity) ToCborIdentity() *CborIdentity {
	return &CborIdentity{
		ID:         i.ID,
		PublicKey:  hex.EncodeToString(i.PublicKey),
		Type:       i.Type,
		Signatures: i.Signatures.ToCborIdentitySignature(),
	}
}

// ToIdentity converts a CBOR serializable to a plain Identity object.
func (c *CborIdentity) ToIdentity(provider Interface) (*Identity, error) {
	publicKey, err := hex.DecodeString(c.PublicKey)
	if err != nil {
		return nil, err
	}

	idSignatures, err := c.Signatures.ToIdentitySignature()
	if err != nil {
		return nil, err
	}

	return &Identity{
		Signatures: idSignatures,
		PublicKey:  publicKey,
		Type:       c.Type,
		ID:         c.ID,
		Provider:   provider,
	}, nil
}

// ToCborIdentitySignature converts to a CBOR serialized identity signature a plain IdentitySignature.
func (i *IdentitySignature) ToCborIdentitySignature() *CborIdentitySignature {
	return &CborIdentitySignature{
		ID:        hex.EncodeToString(i.ID),
		PublicKey: hex.EncodeToString(i.PublicKey),
	}
}

// ToIdentitySignature converts a CBOR serializable identity signature to a plain IdentitySignature.
func (c *CborIdentitySignature) ToIdentitySignature() (*IdentitySignature, error) {
	publicKey, err := hex.DecodeString(c.PublicKey)
	if err != nil {
		return nil, err
	}

	id, err := hex.DecodeString(c.ID)
	if err != nil {
		return nil, err
	}

	return &IdentitySignature{
		PublicKey: publicKey,
		ID:        id,
	}, nil
}
