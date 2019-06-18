package identityprovider // import "berty.tech/go-ipfs-log/identityprovider"

import (
	"encoding/hex"
	"fmt"

	"berty.tech/go-ipfs-log/keystore"
	"github.com/pkg/errors"
)

type OrbitDBIdentityProvider struct {
	keystore keystore.Interface
}

// VerifyIdentity checks an OrbitDB identity.
func (p *OrbitDBIdentityProvider) VerifyIdentity(identity *Identity) error {
	panic("implement me")
}

// NewOrbitDBIdentityProvider creates a new identity for use with OrbitDB.
func NewOrbitDBIdentityProvider(options *CreateIdentityOptions) Interface {
	return &OrbitDBIdentityProvider{
		keystore: options.Keystore,
	}
}

// GetID returns the identity's ID.
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

// SignIdentity signs an OrbitDB identity.
func (p *OrbitDBIdentityProvider) SignIdentity(data []byte, id string) ([]byte, error) {
	key, err := p.keystore.GetKey(id)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Signing key for %s not found", id))
	}

	//data, _ = hex.DecodeString(hex.EncodeToString(data))

	// FIXME? Data is a unicode encoded hex as a byte (source lib uses Buffer.from(hexStr) instead of Buffer.from(hexStr, "hex"))
	data = []byte(hex.EncodeToString(data))

	signature, err := key.Sign(data)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Signing key for %s not found", id))
	}

	return signature, nil
}

// Sign signs a value using the current.
func (p *OrbitDBIdentityProvider) Sign(identity *Identity, data []byte) ([]byte, error) {
	key, err := p.keystore.GetKey(identity.ID)
	if err != nil {
		return nil, errors.Wrap(err, "private signing key not found from Keystore")
	}

	sig, err := key.Sign(data)
	if err != nil {
		return nil, err
	}

	return sig, nil
}

// GetType returns the current identity type.
func (*OrbitDBIdentityProvider) GetType() string {
	return "orbitdb"
}

var _ Interface = &OrbitDBIdentityProvider{}
