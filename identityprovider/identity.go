package identityprovider

import (
	"encoding/base64"
	ic "github.com/libp2p/go-libp2p-crypto"
	pb "github.com/libp2p/go-libp2p-crypto/pb"
)

type IdentitySignature struct {
	ID []byte
	PublicKey ic.PubKey
}

type Identity struct {
	ID         string
	PublicKey  ic.PubKey
	PrivateKey ic.PrivKey
	Signatures *IdentitySignature
	Type       pb.KeyType
	Provider   Interface
}

func FromPrivateKey(key ic.PrivKey) (*Identity, error) {
	pubKey := key.GetPublic()
	pubKeyBytes, err := pubKey.Bytes()
	if err != nil {
		return nil, err
	}

	keyID := base64.StdEncoding.EncodeToString(pubKeyBytes)
	keySign, err := key.Sign(pubKeyBytes)
	if err != nil {
		return nil, err
	}

	return &Identity{
		ID: keyID,
		PublicKey: pubKey,
		Signatures: &IdentitySignature{
			ID: keySign,
			PublicKey: pubKey,
		},
		PrivateKey: key,
		Type:       key.Type(),
	}, nil
}

func (i *Identity) Filtered() * Identity {
	return &Identity{
		ID: i.ID,
		PublicKey: i.PublicKey,
		Signatures: i.Signatures,
		Type: i.Type,
	}
}