package identityprovider

import (
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

func (i *Identity) Filtered() * Identity {
	return &Identity{
		ID: i.ID,
		PublicKey: i.PublicKey,
		Signatures: i.Signatures,
		Type: i.Type,
	}
}