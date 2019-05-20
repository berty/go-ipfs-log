package identityprovider

import (
	"encoding/base64"
	cbornode "github.com/ipfs/go-ipld-cbor"
	ic "github.com/libp2p/go-libp2p-crypto"
	pb "github.com/libp2p/go-libp2p-crypto/pb"
	"github.com/pkg/errors"
	"github.com/polydawn/refmt/obj/atlas"
)

type IdentitySignature struct {
	ID []byte
	PublicKey *ic.Secp256k1PublicKey
}

type Identity struct {
	ID         string
	PublicKey  *ic.Secp256k1PublicKey
	PrivateKey *ic.Secp256k1PrivateKey
	Signatures *IdentitySignature
	Type       pb.KeyType
	Provider   Interface
}

func (i *Identity) Filtered() * Identity {
	return &Identity{
		ID: i.ID,
		PublicKey: i.PublicKey,
		Signatures: i.Signatures,
		Type: i.Type,
	}
}

var AtlasIdentity = atlas.BuildEntry(Identity{}).
	StructMap().
	AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
	AddField("PublicKey", atlas.StructMapEntry{SerialName: "publicKey"}).
	AddField("Signatures", atlas.StructMapEntry{SerialName: "signatures"}).
	AddField("Type", atlas.StructMapEntry{SerialName: "type"}).
	Complete()

var AtlasIdentitySignature = atlas.BuildEntry(IdentitySignature{}).
	StructMap().
	AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
	AddField("PublicKey", atlas.StructMapEntry{SerialName: "publicKey"}).
	Complete()

var AtlasPubKey = atlas.BuildEntry(ic.Secp256k1PublicKey{}).
	Transform().
	TransformMarshal(atlas.MakeMarshalTransformFunc(
		func(x ic.Secp256k1PublicKey) (string, error) {
			keyBytes, err := x.Bytes()
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

			key, err := ic.UnmarshalPublicKey(keyBytes)
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
