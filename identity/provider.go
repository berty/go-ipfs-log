package identity

import "github.com/libp2p/go-libp2p-core/crypto"

type Provider interface {
	// GetID returns id of identity (to be signed by orbit-db public key).
	GetID(id string) (string, error)

	// SignIdentity returns signature of OrbitDB public key signature.
	SignIdentity(data []byte, id string) ([]byte, error)

	// GetType returns the type for this identity provider.
	GetType() string

	// VerifyIdentity checks an identity.
	VerifyIdentity(identity *Identity) error

	// Sign will sign a value.
	Sign(identity *Identity, bytes []byte) ([]byte, error)

	// UnmarshalPublicKey will provide a crypto.PubKey from a key bytes.
	UnmarshalPublicKey(data []byte) (crypto.PubKey, error)
}
