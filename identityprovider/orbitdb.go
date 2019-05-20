package identityprovider

import (
	"fmt"
	"github.com/berty/go-ipfs-log/keystore"
	"github.com/libp2p/go-libp2p-crypto"
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

func (p *OrbitDBIdentityProvider) GetID(id string) (crypto.PrivKey, error) {
	private, err := p.keystore.GetKey(id)
	if err != nil {
		private, err = p.keystore.CreateKey(id)
		if err != nil {
			return nil, err
		}
	}

	return private, nil
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
