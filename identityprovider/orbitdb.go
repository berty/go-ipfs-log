package identityprovider

import (
	"encoding/hex"
	"fmt"
	"github.com/berty/go-ipfs-log/keystore"
	"github.com/pkg/errors"
)

type OrbitDBIdentityProvider struct {
	keystore keystore.Interface
}

func (p *OrbitDBIdentityProvider) VerifyIdentity(identity *Identity) error {
	panic("implement me")
}

func NewOrbitDBIdentityProvider(options *CreateIdentityOptions) Interface {
	return &OrbitDBIdentityProvider{
		keystore: options.Keystore,
	}
}

func (p *OrbitDBIdentityProvider) GetID(options *CreateIdentityOptions) (string, error) {
	private, err := p.keystore.GetKey(options.ID)
	if err != nil || private == nil {
		private, err = p.keystore.CreateKey(options.ID)
		if err != nil {
			return "", err
		}
	}

	pubBytes, err := private.GetPublic().Raw()
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(pubBytes), nil
}

func (p *OrbitDBIdentityProvider) SignIdentity(data []byte, id string) ([]byte, error) {
	key, err := p.keystore.GetKey(id)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Signing key for %s not found", id))
	}

	//data, _ = hex.DecodeString(hex.EncodeToString(data))

	signature, err := key.Sign(data)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Signing key for %s not found", id))
	}

	privKB, _ := key.Raw()

	// TODO: fails here, not consistent with JS behaviour

	fmt.Println("SignIdentity")
	fmt.Println(data)
	fmt.Println(hex.EncodeToString(privKB))
	fmt.Println(hex.EncodeToString(signature))
	fmt.Println("-------")

	return signature, nil
}

func (p *OrbitDBIdentityProvider) Sign(identity *Identity, data []byte) ([]byte, error) {
	key, err := p.keystore.GetKey(identity.ID)
	if err != nil {
		return nil, errors.New("Private signing key not found from Keystore")
	}

	sig, err := key.Sign(data)
	if err != nil {
		return nil, err
	}

	return sig, nil
}

func (*OrbitDBIdentityProvider) GetType() string {
	return "orbitdb"
}

var _ Interface = &OrbitDBIdentityProvider{}
