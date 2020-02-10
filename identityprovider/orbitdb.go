package identityprovider // import "berty.tech/go-ipfs-log/identityprovider"

import (
	"encoding/hex"

	"github.com/libp2p/go-libp2p-core/crypto"

	"berty.tech/go-ipfs-log/errmsg"
	"berty.tech/go-ipfs-log/keystore"
)

type OrbitDBIdentityProvider struct {
	keystore keystore.Interface
}

// VerifyIdentity checks an OrbitDB identity.
func (p *OrbitDBIdentityProvider) VerifyIdentity(identity *Identity) error {
	return nil
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
		return nil, errmsg.KeyNotInKeystore
	}

	//data, _ = hex.DecodeString(hex.EncodeToString(data))

	// FIXME? Data is a unicode encoded hex as a byte (source lib uses Buffer.from(hexStr) instead of Buffer.from(hexStr, "hex"))
	data = []byte(hex.EncodeToString(data))

	signature, err := key.Sign(data)
	if err != nil {
		return nil, errmsg.KeyNotInKeystore
	}

	return signature, nil
}

// Sign signs a value using the current.
func (p *OrbitDBIdentityProvider) Sign(identity *Identity, data []byte) ([]byte, error) {
	key, err := p.keystore.GetKey(identity.ID)
	if err != nil {
		return nil, errmsg.KeyNotInKeystore.Wrap(err)
	}

	sig, err := key.Sign(data)
	if err != nil {
		return nil, err
	}

	return sig, nil
}

func (p *OrbitDBIdentityProvider) UnmarshalPublicKey(data []byte) (crypto.PubKey, error) {
	pubKey, err := crypto.UnmarshalSecp256k1PublicKey(data)
	if err != nil {
		return nil, errmsg.InvalidPubKeyFormat
	}

	return pubKey, nil
}

// GetType returns the current identity type.
func (*OrbitDBIdentityProvider) GetType() string {
	return "orbitdb"
}

var _ Interface = &OrbitDBIdentityProvider{}
