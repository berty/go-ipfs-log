package identityprovider

import (
	"encoding/base64"
	"encoding/hex"
	cbornode "github.com/ipfs/go-ipld-cbor"
	ic "github.com/libp2p/go-libp2p-crypto"
	"github.com/pkg/errors"
	"github.com/polydawn/refmt/obj/atlas"
)

type IdentitySignature struct {
	ID []byte
	PublicKey []byte
}

type CborIdentitySignature struct {
	ID string
	PublicKey string
}

type Identity struct {
	ID         string
	PublicKey  []byte
	Signatures *IdentitySignature
	Type       string
	Provider   Interface
}

type CborIdentity struct {
	ID         string
	PublicKey  string
	Signatures *CborIdentitySignature
	Type       string
}

func (i *Identity) Filtered() * Identity {
	return &Identity{
		ID: i.ID,
		PublicKey: i.PublicKey,
		Signatures: i.Signatures,
		Type: i.Type,
	}
}

func (i *Identity) GetPublicKey() (ic.PubKey, error) {
	return ic.UnmarshalPublicKey(i.PublicKey)
}

var AtlasIdentity = atlas.BuildEntry(CborIdentity{}).
	StructMap().
	AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
	AddField("Type", atlas.StructMapEntry{SerialName: "type"}).
	AddField("PublicKey", atlas.StructMapEntry{SerialName: "publicKey"}).
	AddField("Signatures", atlas.StructMapEntry{SerialName: "signatures"}).
	Complete()

var AtlasIdentitySignature = atlas.BuildEntry(CborIdentitySignature{}).
	StructMap().
	AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
	AddField("PublicKey", atlas.StructMapEntry{SerialName: "publicKey"}).
	Complete()

var AtlasPubKey = atlas.BuildEntry(ic.Secp256k1PublicKey{}).
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
			secpKey, ok := key.(*ic.Secp256k1PublicKey)
			if !ok {
				return ic.Secp256k1PublicKey{}, errors.New("invalid public key")
			}

			return *secpKey, nil
		})).
	Complete()

func init() {
	cbornode.RegisterCborType(AtlasIdentity)
	cbornode.RegisterCborType(AtlasIdentitySignature)
	cbornode.RegisterCborType(AtlasPubKey)
}


func (i *Identity) ToCborIdentity() *CborIdentity {
	return &CborIdentity{
		ID: i.ID,
		PublicKey: hex.EncodeToString(i.PublicKey),
		Type: i.Type,
		Signatures: i.Signatures.ToCborIdentitySignatures(),
	}
}

func (c *CborIdentity) ToIdentity(provider Interface) (*Identity, error) {
	publicKey, err := hex.DecodeString(c.PublicKey)
	if err != nil {
		return nil, err
	}

	idSignatures, err := c.Signatures.ToIdentitySignatures()
	if err != nil {
		return nil, err
	}

	return &Identity{
		Signatures: idSignatures,
		PublicKey: publicKey,
		Type: c.Type,
		ID: c.ID,
		Provider: provider,
	}, nil
}


func (i *IdentitySignature) ToCborIdentitySignatures() *CborIdentitySignature {
	return &CborIdentitySignature{
		ID: hex.EncodeToString(i.ID),
		PublicKey: hex.EncodeToString(i.PublicKey),
	}
}

func (c *CborIdentitySignature) ToIdentitySignatures() (*IdentitySignature, error) {
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
		ID: id,
	}, nil
}


