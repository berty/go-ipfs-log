package identity

import (
	"encoding/hex"
	"fmt"

	"berty.tech/go-ipfs-log/keystore"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/pkg/errors"
)

// NewOrbitDBProvider creates a new identity for use with OrbitDB.
func NewOrbitDBProvider(ks keystore.Keystore) Provider {
	return &OrbitDBProvider{keystore: ks}
}

type OrbitDBProvider struct {
	keystore keystore.Keystore
}

// VerifyIdentity checks an OrbitDB identity.
func (p *OrbitDBProvider) VerifyIdentity(identity *Identity) error {
	return nil
}

// GetID returns the identity's ID.
func (p *OrbitDBProvider) GetID(id string) (string, error) {
	// FIXME: confusing input/output variable names
	private, err := p.keystore.GetKey(id)
	if err != nil || private == nil {
		private, err = p.keystore.CreateKey(id)
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
func (p *OrbitDBProvider) SignIdentity(data []byte, id string) ([]byte, error) {
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
func (p *OrbitDBProvider) Sign(identity *Identity, data []byte) ([]byte, error) {
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

func (p *OrbitDBProvider) UnmarshalPublicKey(data []byte) (crypto.PubKey, error) {
	pubKey, err := crypto.UnmarshalSecp256k1PublicKey(data)
	if err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal public key")
	}

	return pubKey, nil
}

// GetType returns the current identity type.
func (*OrbitDBProvider) GetType() string {
	return "orbitdb"
}

var _ Provider = &OrbitDBProvider{}
