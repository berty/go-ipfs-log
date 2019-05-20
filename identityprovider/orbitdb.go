package identityprovider

import (
	"fmt"
	"github.com/berty/go-ipfs-log/keystore"
	crypto "github.com/libp2p/go-libp2p-crypto"
	"github.com/pkg/errors"
)

type OrbitDBIdentityProvider struct {
	keystore keystore.Interface
}

func NewOrbitDBIdentityProvider(keystore keystore.Interface) *OrbitDBIdentityProvider {
	return &OrbitDBIdentityProvider{
		keystore: keystore,
	}
}

func (p *OrbitDBIdentityProvider) GetID(id string) (*Identity, error) {
	private, err := p.keystore.GetKey(id)
	if err != nil {
		private, err = p.keystore.CreateKey(id)
		if err != nil {
			return nil, err
		}
	}

	pubKey := private.GetPublic()
	pubKeyBytes, err := pubKey.Bytes()
	if err != nil {
		return nil, err
	}

	keySign, err := private.Sign(pubKeyBytes)
	if err != nil {
		return nil, err
	}

	return &Identity{
		ID:        id,
		PublicKey: pubKey.(*crypto.Secp256k1PublicKey),
		Signatures: &IdentitySignature{
			ID:        keySign,
			PublicKey: pubKey.(*crypto.Secp256k1PublicKey),
		},
		PrivateKey: private,
		Type:       private.Type(),
		Provider:   p,
	}, nil
}

func (p *OrbitDBIdentityProvider) SignIdentity(data []byte, id string) ([]byte, error) {
	key, err := p.keystore.GetKey(id)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Signing key for %s not found", id))
	}

	return key.Sign(data)
}

func (*OrbitDBIdentityProvider) GetType() string {
	return "OrbitDBIdentityProvider"
}

var _ Interface = &OrbitDBIdentityProvider{}
